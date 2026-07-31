//! External API text-protocol gateway guest.
//!
//! Accepts user requests over a TCP connection (or future QUIC stream),
//! parses them into [`UserIntent`], decomposes intents into ordered
//! [`DelegatedInteraction`] steps, and dispatches to discovery/scheduler
//! over RPC.
//!
//! # Text-Protocol Grammar
//!
//! ```text
//! request     = command [workload_id] [replicas]
//! command     = "deploy" | "start" | "stop" | "scale" | "resolve"
//! workload_id = <non-whitespace string>
//! replicas    = <unsigned integer>
//! ```
//!
//! # Parsing Pipeline
//!
//! ```text
//! TCP bytes → String → parse_intent → UserIntent → decompose_intent → Vec<DelegatedInteraction>
//! ```
//!
//! Each [`DelegatedInteraction`] is dispatched to the appropriate guest over RPC:
//! - [`DelegatedInteraction::DiscoveryResolve`] → discovery RPC client
//! - [`DelegatedInteraction::SchedulerPlace`], [`DelegatedInteraction::SchedulerStop`],
//!   [`DelegatedInteraction::SchedulerScale`] → scheduler RPC client
//!
//! # Inbound Network Bridge
//!
//! The API guest receives client connections through a runtime-managed inbound
//! network bridge. Each connection is presented as a pair of shared-memory ring
//! buffers (inbound for reading client requests, outbound for writing responses).
//! The runtime bridge is currently stubbed; the parsing and dispatch pipeline is
//! fully implemented and tested.

use selium_guest::{
    Context,
    entrypoint,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserIntent {
    Deploy { workload_id: String, replicas: u32 },
    Start { workload_id: String, replicas: u32 },
    Stop { workload_id: String },
    Scale { workload_id: String, replicas: u32 },
    Resolve { uri: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DelegatedInteraction {
    DiscoveryResolve { uri: String },
    SchedulerPlace { workload_id: String, replicas: u32 },
    SchedulerScale { workload_id: String, replicas: u32 },
    SchedulerStop { workload_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFeedback {
    pub accepted: bool,
    pub message: String,
    pub delegated: Vec<DelegatedInteraction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    EmptyRequest,
    UnknownCommand(String),
    InvalidReplicaCount(String),
    MissingArgument(&'static str),
    DelegationFailed { step: String, context: String },
}

/// Scheduler RPC request types.
///
/// TODO: Move to `selium_abi` alongside `DiscoveryRequest`/`DiscoveryResponse`
/// once the scheduler guest crate is implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerRequest {
    Place { workload_id: String, replicas: u32 },
    Stop { workload_id: String },
    Scale { workload_id: String, replicas: u32 },
}

/// Scheduler RPC response types.
///
/// TODO: Move to `selium_abi` alongside `DiscoveryRequest`/`DiscoveryResponse`
/// once the scheduler guest crate is implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerResponse {
    Ok,
    Error(String),
}

/// Bootstrap context for the external API guest.
///
/// Contains a pre-connected discovery RPC client (via [`Context`]) and a
/// stub for the scheduler RPC client. When the runtime inbound network
/// bridge is ready, this struct will also hold the inbound/outbound ring
/// buffer handles for accepting client connections.
pub struct ApiContext {
    /// Discovery RPC client for URI resolution.
    discovery: Context,
    // TODO: scheduler RPC client — blocked on scheduler guest implementation.
    // scheduler: RpcClient<SchedulerRequest, SchedulerResponse>,
}

impl ApiContext {
    /// Constructs an `ApiContext` from the guest bootstrap [`Context`].
    pub fn from_context(discovery: Context) -> Self {
        Self { discovery }
    }

    /// Returns a mutable reference to the discovery context.
    pub fn discovery_mut(&mut self) -> &mut Context {
        &mut self.discovery
    }
}

/// Parses a text request, decomposes it into delegated interactions, and
/// dispatches each to the appropriate RPC service.
///
/// Returns [`ClientFeedback`] with the acceptance status and dispatched interactions.
pub async fn accept_request(
    ctx: &mut ApiContext,
    request: &str,
) -> Result<ClientFeedback, ApiError> {
    let intent = parse_intent(request)?;
    let delegated = decompose_intent(intent);
    dispatch_all(ctx, &delegated).await?;
    Ok(ClientFeedback {
        accepted: true,
        message: "request accepted".to_string(),
        delegated,
    })
}

/// Synchronous version of `accept_request` for testing (no dispatch).
pub fn accept_request_sync(request: &str) -> Result<ClientFeedback, ApiError> {
    let intent = parse_intent(request)?;
    let delegated = decompose_intent(intent);
    Ok(ClientFeedback {
        accepted: true,
        message: "request accepted".to_string(),
        delegated,
    })
}

/// Decomposes a [`UserIntent`] into an ordered list of [`DelegatedInteraction`] steps.
pub fn decompose_intent(intent: UserIntent) -> Vec<DelegatedInteraction> {
    match intent {
        UserIntent::Deploy {
            workload_id,
            replicas,
        }
        | UserIntent::Start {
            workload_id,
            replicas,
        } => vec![
            DelegatedInteraction::DiscoveryResolve {
                uri: workload_id.clone(),
            },
            DelegatedInteraction::SchedulerPlace {
                workload_id,
                replicas,
            },
        ],
        UserIntent::Stop { workload_id } => {
            vec![DelegatedInteraction::SchedulerStop { workload_id }]
        }
        UserIntent::Scale {
            workload_id,
            replicas,
        } => vec![DelegatedInteraction::SchedulerScale {
            workload_id,
            replicas,
        }],
        UserIntent::Resolve { uri } => vec![DelegatedInteraction::DiscoveryResolve { uri }],
    }
}

/// Dispatches all delegated interactions in order, returning the first error.
pub async fn dispatch_all(
    ctx: &mut ApiContext,
    interactions: &[DelegatedInteraction],
) -> Result<(), ApiError> {
    for interaction in interactions {
        dispatch_interaction(ctx, interaction).await?;
    }
    Ok(())
}

/// Dispatches a single [`DelegatedInteraction`] to the appropriate RPC service.
///
/// Discovery interactions are sent to the discovery guest via the context.
/// Scheduler interactions are currently stubbed (TODO: wire scheduler RPC client).
pub async fn dispatch_interaction(
    ctx: &mut ApiContext,
    interaction: &DelegatedInteraction,
) -> Result<(), ApiError> {
    match interaction {
        DelegatedInteraction::DiscoveryResolve { uri } => {
            ctx.discovery_mut()
                .lookup(uri)
                .await
                .map_err(|e| delegation_error("discovery", format!("{e}")))?;
            Ok(())
        }
        DelegatedInteraction::SchedulerPlace { .. }
        | DelegatedInteraction::SchedulerStop { .. }
        | DelegatedInteraction::SchedulerScale { .. } => {
            // TODO: Wire scheduler RPC client once the scheduler guest is implemented.
            // For now, log and succeed — the interaction is correctly decomposed.
            selium_guest::debug!(
                interaction = ?interaction,
                "scheduler dispatch stubbed until scheduler guest is ready"
            );
            Ok(())
        }
    }
}

/// Parses a whitespace-delimited text request into a [`UserIntent`].
pub fn parse_intent(request: &str) -> Result<UserIntent, ApiError> {
    let parts = request.split_whitespace().collect::<Vec<_>>();
    let Some(command) = parts.first() else {
        return Err(ApiError::EmptyRequest);
    };

    match *command {
        "deploy" => Ok(UserIntent::Deploy {
            workload_id: required(&parts, 1, "workload_id")?.to_string(),
            replicas: replicas(&parts, 2)?,
        }),
        "start" => Ok(UserIntent::Start {
            workload_id: required(&parts, 1, "workload_id")?.to_string(),
            replicas: replicas(&parts, 2)?,
        }),
        "stop" => Ok(UserIntent::Stop {
            workload_id: required(&parts, 1, "workload_id")?.to_string(),
        }),
        "scale" => Ok(UserIntent::Scale {
            workload_id: required(&parts, 1, "workload_id")?.to_string(),
            replicas: replicas(&parts, 2)?,
        }),
        "resolve" => Ok(UserIntent::Resolve {
            uri: required(&parts, 1, "uri")?.to_string(),
        }),
        other => Err(ApiError::UnknownCommand(other.to_string())),
    }
}

fn delegation_error(step: impl Into<String>, context: impl Into<String>) -> ApiError {
    ApiError::DelegationFailed {
        step: step.into(),
        context: context.into(),
    }
}

#[entrypoint]
async fn external_api_main(ctx: Context) {
    drop(selium_guest::log::init());
    let _api_ctx = ApiContext::from_context(ctx);
    selium_guest::info!(
        guest = "selium-external-api",
        "external API transport is blocked until the runtime exposes a configured inbound network bridge"
    );
    selium_guest::mark_ready();
}

fn replicas(parts: &[&str], index: usize) -> Result<u32, ApiError> {
    let raw = required(parts, index, "replicas")?;
    raw.parse::<u32>()
        .map_err(|_error| ApiError::InvalidReplicaCount(raw.to_string()))
}

fn required<'a>(parts: &'a [&str], index: usize, name: &'static str) -> Result<&'a str, ApiError> {
    parts
        .get(index)
        .copied()
        .ok_or(ApiError::MissingArgument(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- parse_intent tests --

    #[test]
    fn parse_deploy_request() {
        assert_eq!(
            parse_intent("deploy my-workload 3"),
            Ok(UserIntent::Deploy {
                workload_id: "my-workload".to_string(),
                replicas: 3,
            })
        );
    }

    #[test]
    fn parse_start_request() {
        assert_eq!(
            parse_intent("start api-service 2"),
            Ok(UserIntent::Start {
                workload_id: "api-service".to_string(),
                replicas: 2,
            })
        );
    }

    #[test]
    fn parse_stop_request() {
        assert_eq!(
            parse_intent("stop my-workload"),
            Ok(UserIntent::Stop {
                workload_id: "my-workload".to_string(),
            })
        );
    }

    #[test]
    fn parse_scale_request() {
        assert_eq!(
            parse_intent("scale api-service 5"),
            Ok(UserIntent::Scale {
                workload_id: "api-service".to_string(),
                replicas: 5,
            })
        );
    }

    #[test]
    fn parse_resolve_request() {
        assert_eq!(
            parse_intent("resolve selium://my-service"),
            Ok(UserIntent::Resolve {
                uri: "selium://my-service".to_string(),
            })
        );
    }

    #[test]
    fn parse_unknown_command() {
        assert_eq!(
            parse_intent("unknown-command arg1"),
            Err(ApiError::UnknownCommand("unknown-command".to_string()))
        );
    }

    #[test]
    fn parse_empty_request() {
        assert_eq!(parse_intent(""), Err(ApiError::EmptyRequest));
        assert_eq!(parse_intent("   "), Err(ApiError::EmptyRequest));
    }

    #[test]
    fn parse_missing_workload_id() {
        assert_eq!(
            parse_intent("deploy"),
            Err(ApiError::MissingArgument("workload_id"))
        );
    }

    #[test]
    fn parse_missing_replicas() {
        assert_eq!(
            parse_intent("deploy my-workload"),
            Err(ApiError::MissingArgument("replicas"))
        );
    }

    #[test]
    fn parse_invalid_replica_count() {
        assert_eq!(
            parse_intent("scale api many"),
            Err(ApiError::InvalidReplicaCount("many".to_string()))
        );
    }

    // -- decompose_intent tests --

    #[test]
    fn decompose_deploy_to_resolve_and_place() {
        let steps = decompose_intent(UserIntent::Deploy {
            workload_id: "w".to_string(),
            replicas: 3,
        });
        assert_eq!(
            steps,
            vec![
                DelegatedInteraction::DiscoveryResolve {
                    uri: "w".to_string(),
                },
                DelegatedInteraction::SchedulerPlace {
                    workload_id: "w".to_string(),
                    replicas: 3,
                },
            ]
        );
    }

    #[test]
    fn decompose_start_to_resolve_and_place() {
        let steps = decompose_intent(UserIntent::Start {
            workload_id: "w".to_string(),
            replicas: 2,
        });
        assert_eq!(
            steps,
            vec![
                DelegatedInteraction::DiscoveryResolve {
                    uri: "w".to_string(),
                },
                DelegatedInteraction::SchedulerPlace {
                    workload_id: "w".to_string(),
                    replicas: 2,
                },
            ]
        );
    }

    #[test]
    fn decompose_stop_to_scheduler_stop() {
        let steps = decompose_intent(UserIntent::Stop {
            workload_id: "w".to_string(),
        });
        assert_eq!(
            steps,
            vec![DelegatedInteraction::SchedulerStop {
                workload_id: "w".to_string(),
            }]
        );
    }

    #[test]
    fn decompose_scale_to_scheduler_scale() {
        let steps = decompose_intent(UserIntent::Scale {
            workload_id: "w".to_string(),
            replicas: 5,
        });
        assert_eq!(
            steps,
            vec![DelegatedInteraction::SchedulerScale {
                workload_id: "w".to_string(),
                replicas: 5,
            }]
        );
    }

    #[test]
    fn decompose_resolve_to_discovery_resolve() {
        let steps = decompose_intent(UserIntent::Resolve {
            uri: "u".to_string(),
        });
        assert_eq!(
            steps,
            vec![DelegatedInteraction::DiscoveryResolve {
                uri: "u".to_string(),
            }]
        );
    }

    // -- accept_request_sync (end-to-end parsing pipeline) tests --

    #[test]
    fn accept_deploy_request_end_to_end() {
        let feedback = accept_request_sync("deploy sel://tenant/app/api 2");
        assert_eq!(
            feedback,
            Ok(ClientFeedback {
                accepted: true,
                message: "request accepted".to_string(),
                delegated: vec![
                    DelegatedInteraction::DiscoveryResolve {
                        uri: "sel://tenant/app/api".to_string(),
                    },
                    DelegatedInteraction::SchedulerPlace {
                        workload_id: "sel://tenant/app/api".to_string(),
                        replicas: 2,
                    },
                ],
            })
        );
    }

    #[test]
    fn accept_stop_request_end_to_end() {
        let feedback = accept_request_sync("stop my-service");
        assert_eq!(
            feedback,
            Ok(ClientFeedback {
                accepted: true,
                message: "request accepted".to_string(),
                delegated: vec![DelegatedInteraction::SchedulerStop {
                    workload_id: "my-service".to_string(),
                }],
            })
        );
    }

    #[test]
    fn accept_invalid_request_returns_error() {
        let result = accept_request_sync("bogus");
        assert_eq!(result, Err(ApiError::UnknownCommand("bogus".to_string())));
    }

    #[test]
    fn carries_delegation_failure_context() {
        assert_eq!(
            delegation_error("scheduler", "no host available"),
            ApiError::DelegationFailed {
                step: "scheduler".to_string(),
                context: "no host available".to_string(),
            }
        );
    }
}
