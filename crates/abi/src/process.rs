use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    AbiParam, AbiScalarType, AbiScalarValue, AbiSignature, CallPlanError, GuestResourceId,
};

/// Runtime-managed stream used for guest log forwarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum GuestLogStream {
    Stdout,
    Stderr,
}

impl GuestLogStream {
    pub const fn as_raw(self) -> u32 {
        match self {
            Self::Stdout => 1,
            Self::Stderr => 2,
        }
    }

    pub const fn from_raw(raw: u32) -> Option<Self> {
        match raw {
            1 => Some(Self::Stdout),
            2 => Some(Self::Stderr),
            _ => None,
        }
    }
}

/// Runtime-managed stdout/stderr queues attached to a process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProcessLogBindings {
    pub stdout_queue_shared_id: Option<GuestResourceId>,
    pub stderr_queue_shared_id: Option<GuestResourceId>,
}

impl ProcessLogBindings {
    pub const fn is_empty(self) -> bool {
        self.stdout_queue_shared_id.is_none() && self.stderr_queue_shared_id.is_none()
    }

    pub const fn queue_shared_id(self, stream: GuestLogStream) -> Option<GuestResourceId> {
        match stream {
            GuestLogStream::Stdout => self.stdout_queue_shared_id,
            GuestLogStream::Stderr => self.stderr_queue_shared_id,
        }
    }
}

/// Argument supplied to a process entrypoint.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum EntrypointArg {
    /// Immediate scalar value.
    Scalar(AbiScalarValue),
    /// Raw buffer passed using the signature's buffer parameter convention.
    Buffer(Vec<u8>),
    /// Handle referring to a Selium resource.
    ///
    /// Use this when an entrypoint parameter semantically expects a resource handle rather than a
    /// caller-chosen integer. Validation currently permits resource handles for `i32` and `u64`
    /// scalar slots.
    Resource(GuestResourceId),
}

/// Invocation of a process entrypoint.
///
/// This pairs an [`AbiSignature`] with the concrete guest arguments a process should receive.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EntrypointInvocation {
    /// ABI signature describing the entrypoint.
    pub signature: AbiSignature,
    /// Concrete arguments supplied by the caller.
    pub args: Vec<EntrypointArg>,
}

impl EntrypointInvocation {
    /// Construct an invocation, validating that arguments satisfy the signature.
    pub fn new(signature: AbiSignature, args: Vec<EntrypointArg>) -> Result<Self, CallPlanError> {
        let invocation = Self { signature, args };
        invocation.validate()?;
        Ok(invocation)
    }

    /// Validate that arguments align with the ABI signature.
    pub fn validate(&self) -> Result<(), CallPlanError> {
        if self.signature.params().len() != self.args.len() {
            return Err(CallPlanError::ParameterCount {
                expected: self.signature.params().len(),
                actual: self.args.len(),
            });
        }

        for (index, (param, arg)) in self
            .signature
            .params()
            .iter()
            .zip(self.args.iter())
            .enumerate()
        {
            match (param, arg) {
                (AbiParam::Scalar(expected), EntrypointArg::Scalar(actual)) => {
                    if actual.kind() != *expected {
                        return Err(CallPlanError::ValueMismatch {
                            index,
                            reason: "scalar type mismatch",
                        });
                    }
                }
                (AbiParam::Scalar(AbiScalarType::I32), EntrypointArg::Resource(_))
                | (AbiParam::Scalar(AbiScalarType::U64), EntrypointArg::Resource(_)) => {}
                (AbiParam::Buffer, EntrypointArg::Buffer(_)) => {}
                _ => {
                    return Err(CallPlanError::ValueMismatch {
                        index,
                        reason: "argument incompatible with signature",
                    });
                }
            }
        }

        Ok(())
    }

    /// Borrow the invocation signature.
    pub fn signature(&self) -> &AbiSignature {
        &self.signature
    }
}

/// Register a process's logging channel with the host.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ProcessLogRegistration {
    /// Shared channel handle exported by the guest.
    pub channel: GuestResourceId,
}

/// Request the logging channel for a running process.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ProcessLogLookup {
    /// Handle referencing the process to inspect.
    pub process_id: GuestResourceId,
}

/// Request to start a new process instance.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ProcessStart {
    /// Module identifier that should be activated.
    pub module_id: String,
    /// Friendly process name.
    pub name: String,
    /// Capabilities granted to the process.
    pub capabilities: Vec<crate::Capability>,
    /// Runtime-managed outbound egress profiles granted to the process.
    pub network_egress_profiles: Vec<String>,
    /// Runtime-managed inbound bindings granted to the process.
    pub network_ingress_bindings: Vec<String>,
    /// Runtime-managed durable logs granted to the process.
    pub storage_logs: Vec<String>,
    /// Runtime-managed blob stores granted to the process.
    pub storage_blobs: Vec<String>,
    /// Entrypoint invocation details.
    pub entrypoint: EntrypointInvocation,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_new_accepts_matching_arguments() {
        let signature = AbiSignature::new(
            vec![
                AbiParam::Scalar(AbiScalarType::I32),
                AbiParam::Buffer,
                AbiParam::Scalar(AbiScalarType::U64),
            ],
            Vec::new(),
        );
        let args = vec![
            EntrypointArg::Scalar(AbiScalarValue::I32(7)),
            EntrypointArg::Buffer(vec![1, 2, 3]),
            EntrypointArg::Resource(99),
        ];

        let invocation = EntrypointInvocation::new(signature, args).expect("valid invocation");
        assert_eq!(invocation.args.len(), 3);
    }

    #[test]
    fn invocation_validate_rejects_argument_count_mismatch() {
        let signature = AbiSignature::new(vec![AbiParam::Scalar(AbiScalarType::I32)], Vec::new());
        let invocation = EntrypointInvocation {
            signature,
            args: Vec::new(),
        };

        let err = invocation.validate().expect_err("expected mismatch");
        assert!(matches!(
            err,
            CallPlanError::ParameterCount {
                expected: 1,
                actual: 0
            }
        ));
    }

    #[test]
    fn invocation_validate_rejects_scalar_type_mismatch() {
        let signature = AbiSignature::new(vec![AbiParam::Scalar(AbiScalarType::I32)], Vec::new());
        let invocation = EntrypointInvocation {
            signature,
            args: vec![EntrypointArg::Scalar(AbiScalarValue::U32(1))],
        };

        let err = invocation.validate().expect_err("expected mismatch");
        assert!(matches!(
            err,
            CallPlanError::ValueMismatch {
                reason: "scalar type mismatch",
                ..
            }
        ));
    }

    #[test]
    fn invocation_validate_accepts_resource_for_i32_and_u64() {
        let signature = AbiSignature::new(
            vec![
                AbiParam::Scalar(AbiScalarType::I32),
                AbiParam::Scalar(AbiScalarType::U64),
            ],
            Vec::new(),
        );
        let invocation = EntrypointInvocation {
            signature,
            args: vec![EntrypointArg::Resource(1), EntrypointArg::Resource(2)],
        };

        invocation.validate().expect("resources are accepted");
    }

    #[test]
    fn guest_log_stream_round_trips_raw_codes() {
        assert_eq!(GuestLogStream::from_raw(1), Some(GuestLogStream::Stdout));
        assert_eq!(GuestLogStream::from_raw(2), Some(GuestLogStream::Stderr));
        assert_eq!(GuestLogStream::Stdout.as_raw(), 1);
        assert_eq!(GuestLogStream::Stderr.as_raw(), 2);
        assert_eq!(GuestLogStream::from_raw(9), None);
    }

    #[test]
    fn process_log_bindings_select_queue_by_stream() {
        let bindings = ProcessLogBindings {
            stdout_queue_shared_id: Some(11),
            stderr_queue_shared_id: Some(22),
        };

        assert!(!bindings.is_empty());
        assert_eq!(bindings.queue_shared_id(GuestLogStream::Stdout), Some(11));
        assert_eq!(bindings.queue_shared_id(GuestLogStream::Stderr), Some(22));
        assert!(ProcessLogBindings::default().is_empty());
    }
}
