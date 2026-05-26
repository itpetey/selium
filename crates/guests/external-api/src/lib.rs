//! External API system guest.

use selium_guest::entrypoint;

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

fn accept_request(request: &str) -> Result<ClientFeedback, ApiError> {
    let intent = parse_intent(request)?;
    let delegated = decompose_intent(intent);
    Ok(ClientFeedback {
        accepted: true,
        message: "request accepted".to_string(),
        delegated,
    })
}

fn decompose_intent(intent: UserIntent) -> Vec<DelegatedInteraction> {
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

fn delegation_error(step: impl Into<String>, context: impl Into<String>) -> ApiError {
    ApiError::DelegationFailed {
        step: step.into(),
        context: context.into(),
    }
}

#[entrypoint]
async fn external_api_main() {
    selium_guest::info!(
        guest = "selium-external-api",
        "external API transport is blocked until the runtime exposes a configured inbound network bridge"
    );
    selium_guest::mark_ready();
}

fn parse_intent(request: &str) -> Result<UserIntent, ApiError> {
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

    #[test]
    fn parses_and_decomposes_deploy_intent() {
        let feedback = accept_request("deploy sel://tenant/app/api 2");

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
    fn reports_parse_errors_with_context() {
        assert_eq!(
            parse_intent("scale api many"),
            Err(ApiError::InvalidReplicaCount("many".to_string()))
        );
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
