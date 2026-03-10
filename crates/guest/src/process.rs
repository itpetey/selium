//! Guest-facing helpers for launching and stopping Selium processes.
//!
//! Use [`ProcessBuilder`] to describe the child module, entrypoint, capabilities, and typed
//! arguments to pass to the runtime. Starting the builder yields a [`ProcessHandle`] that can be
//! used later to stop the launched process.
//!
//! # Examples
//! ```no_run
//! use selium_guest::{
//!     abi::{AbiParam, AbiScalarType, AbiSignature},
//!     process::{Capability, ProcessBuilder, ProcessError},
//! };
//!
//! # async fn example() -> Result<(), ProcessError> {
//! let signature = AbiSignature::new(vec![AbiParam::Scalar(AbiScalarType::I32)], Vec::new());
//! let handle = ProcessBuilder::new("selium.examples.echo", "echoer")
//!     .capability(Capability::TimeRead)
//!     .signature(signature)
//!     .arg_i32(42)
//!     .start()
//!     .await?;
//!
//! handle.stop().await?;
//! # Ok(())
//! # }
//! ```

use selium_abi::{
    AbiScalarValue, AbiSignature, EntrypointArg, EntrypointInvocation, GuestResourceId,
    ProcessStart, RkyvEncode,
};

use crate::driver::{self, DriverFuture, RkyvDecoder, encode_args};

/// Capability identifiers that can be granted to a launched child process.
pub use selium_abi::Capability;

/// Error returned by process lifecycle helpers.
///
/// This covers argument encoding errors as well as host failures reported while starting or
/// stopping a process.
pub type ProcessError = driver::DriverError;

/// Builder for configuring and launching a Selium process.
///
/// Builders are cheap to clone and can be assembled incrementally before calling [`start`](Self::start).
#[derive(Clone, Debug, PartialEq)]
pub struct ProcessBuilder {
    module_id: String,
    entrypoint: String,
    capabilities: Vec<Capability>,
    network_egress_profiles: Vec<String>,
    network_ingress_bindings: Vec<String>,
    storage_logs: Vec<String>,
    storage_blobs: Vec<String>,
    signature: AbiSignature,
    args: Vec<EntrypointArg>,
}

/// Handle representing a running process in the Selium registry.
///
/// Handles are returned by [`ProcessBuilder::start`] and can be passed back to the runtime to stop
/// the child process later.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessHandle(GuestResourceId);

impl ProcessBuilder {
    /// Create a builder for a child module and entrypoint.
    ///
    /// `module_id` identifies the deployed guest module to launch and `entrypoint` names the
    /// exported guest function within that module.
    pub fn new(module_id: impl Into<String>, entrypoint: impl Into<String>) -> Self {
        Self {
            module_id: module_id.into(),
            entrypoint: entrypoint.into(),
            capabilities: Vec::new(),
            network_egress_profiles: Vec::new(),
            network_ingress_bindings: Vec::new(),
            storage_logs: Vec::new(),
            storage_blobs: Vec::new(),
            signature: AbiSignature::new(Vec::new(), Vec::new()),
            args: Vec::new(),
        }
    }

    /// Grant one capability to the launched process.
    ///
    /// Repeated calls with the same capability are ignored.
    pub fn capability(mut self, capability: Capability) -> Self {
        if !self.capabilities.contains(&capability) {
            self.capabilities.push(capability);
        }
        self
    }

    /// Grant access to one runtime-managed outbound egress profile.
    ///
    /// Repeated calls with the same profile name are ignored.
    pub fn network_egress_profile(mut self, profile: impl Into<String>) -> Self {
        let profile = profile.into();
        if !self.network_egress_profiles.contains(&profile) {
            self.network_egress_profiles.push(profile);
        }
        self
    }

    /// Grant access to one runtime-managed inbound binding.
    ///
    /// Repeated calls with the same binding name are ignored.
    pub fn network_ingress_binding(mut self, binding: impl Into<String>) -> Self {
        let binding = binding.into();
        if !self.network_ingress_bindings.contains(&binding) {
            self.network_ingress_bindings.push(binding);
        }
        self
    }

    /// Grant access to one runtime-managed durable log.
    ///
    /// Repeated calls with the same log name are ignored.
    pub fn storage_log(mut self, log: impl Into<String>) -> Self {
        let log = log.into();
        if !self.storage_logs.contains(&log) {
            self.storage_logs.push(log);
        }
        self
    }

    /// Grant access to one runtime-managed blob store.
    ///
    /// Repeated calls with the same blob name are ignored.
    pub fn storage_blob(mut self, blob: impl Into<String>) -> Self {
        let blob = blob.into();
        if !self.storage_blobs.contains(&blob) {
            self.storage_blobs.push(blob);
        }
        self
    }

    /// Specify the target entrypoint ABI signature.
    ///
    /// The signature should match both the `arg_*` values added to this builder and the typed
    /// parameters expected by the launched guest entrypoint.
    pub fn signature(mut self, signature: AbiSignature) -> Self {
        self.signature = signature;
        self
    }

    /// Append one scalar argument to the child entrypoint invocation.
    pub fn arg_scalar(mut self, value: AbiScalarValue) -> Self {
        self.args.push(EntrypointArg::Scalar(value));
        self
    }

    /// Append one 32-bit integer argument.
    pub fn arg_i32(self, value: i32) -> Self {
        self.arg_scalar(AbiScalarValue::I32(value))
    }

    /// Append one 64-bit integer argument.
    pub fn arg_i64(self, value: i64) -> Self {
        self.arg_scalar(AbiScalarValue::I64(value))
    }

    /// Append one 32-bit float argument.
    pub fn arg_f32(self, value: f32) -> Self {
        self.arg_scalar(AbiScalarValue::F32(value))
    }

    /// Append one 64-bit float argument.
    pub fn arg_f64(self, value: f64) -> Self {
        self.arg_scalar(AbiScalarValue::F64(value))
    }

    /// Append one UTF-8 string argument.
    pub fn arg_utf8(self, value: impl Into<String>) -> Self {
        self.arg_buffer(value.into().into_bytes())
    }

    /// Append one argument encoded with Selium's `rkyv` conventions.
    ///
    /// Use this for structured payloads that should be passed as ABI buffers.
    pub fn arg_rkyv<T: RkyvEncode>(mut self, value: &T) -> Result<Self, ProcessError> {
        let bytes = encode_args(value)?;
        self.args.push(EntrypointArg::Buffer(bytes));
        Ok(self)
    }

    /// Append one raw buffer argument.
    pub fn arg_buffer(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.args.push(EntrypointArg::Buffer(value.into()));
        self
    }

    /// Append one resource-handle argument.
    pub fn arg_resource(mut self, handle: impl Into<GuestResourceId>) -> Self {
        self.args.push(EntrypointArg::Resource(handle.into()));
        self
    }

    /// Launch the configured process and return its handle.
    ///
    /// The host validates the supplied capabilities, signature, and argument list before creating
    /// the child process.
    pub async fn start(self) -> Result<ProcessHandle, ProcessError> {
        start_process(self).await
    }
}

impl ProcessHandle {
    /// Return the raw process resource identifier.
    ///
    /// This is mainly useful when storing the handle elsewhere or passing it through lower-level
    /// APIs that work with raw guest resource IDs.
    pub fn raw(&self) -> GuestResourceId {
        self.0
    }

    /// Construct a handle from a raw registry identifier.
    ///
    /// # Safety
    /// The handle must be a valid process capability minted for the current guest. Forged or stale
    /// handles may be rejected by the host kernel or cause undefined behaviour.
    pub unsafe fn from_raw(handle: GuestResourceId) -> Self {
        Self(handle)
    }

    /// Ask the runtime to stop the referenced process.
    pub async fn stop(self) -> Result<(), ProcessError> {
        let args = encode_args(&self.0)?;
        DriverFuture::<process_stop::Module, RkyvDecoder<()>>::new(&args, 0, RkyvDecoder::new())?
            .await
            .map(|_| ())
    }
}

async fn start_process(builder: ProcessBuilder) -> Result<ProcessHandle, ProcessError> {
    let args = encode_start_args(builder)?;
    let handle = DriverFuture::<process_start::Module, RkyvDecoder<GuestResourceId>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await?;
    Ok(ProcessHandle(handle))
}

fn encode_start_args(builder: ProcessBuilder) -> Result<Vec<u8>, ProcessError> {
    let payload = build_start_payload(builder)?;
    encode_args(&payload)
}

fn build_start_payload(builder: ProcessBuilder) -> Result<ProcessStart, ProcessError> {
    let ProcessBuilder {
        module_id,
        entrypoint,
        capabilities,
        network_egress_profiles,
        network_ingress_bindings,
        storage_logs,
        storage_blobs,
        signature,
        args,
    } = builder;

    let invocation =
        EntrypointInvocation::new(signature, args).map_err(|_| ProcessError::InvalidArgument)?;

    Ok(ProcessStart {
        module_id,
        name: entrypoint,
        capabilities,
        network_egress_profiles,
        network_ingress_bindings,
        storage_logs,
        storage_blobs,
        entrypoint: invocation,
    })
}

driver_module!(process_start, "selium::process::start");
driver_module!(process_stop, "selium::process::stop");

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::decode_rkyv;
    use selium_abi::{AbiParam, AbiScalarType};

    #[test]
    fn encode_start_args_serialises_signature_and_arguments() {
        let signature = AbiSignature::new(
            vec![AbiParam::Scalar(AbiScalarType::I32), AbiParam::Buffer],
            vec![AbiParam::Scalar(AbiScalarType::F64)],
        );

        let builder = ProcessBuilder::new("module", "proc")
            .capability(Capability::TimeRead)
            .signature(signature.clone())
            .arg_i32(42)
            .arg_buffer([1, 2, 3]);
        let bytes = encode_start_args(builder).expect("encode");
        let start = decode_rkyv::<ProcessStart>(&bytes).expect("decode");
        assert_eq!(start.module_id, "module");
        assert_eq!(start.name, "proc");
        assert_eq!(start.capabilities, vec![Capability::TimeRead]);
        assert!(start.network_egress_profiles.is_empty());
        assert!(start.network_ingress_bindings.is_empty());
        assert!(start.storage_logs.is_empty());
        assert!(start.storage_blobs.is_empty());
        assert_eq!(start.entrypoint.signature.params(), signature.params());
        assert_eq!(start.entrypoint.signature.results(), signature.results());
        assert_eq!(
            start.entrypoint.args,
            [
                EntrypointArg::Scalar(AbiScalarValue::I32(42)),
                EntrypointArg::Buffer(vec![1, 2, 3])
            ]
        );
    }

    #[test]
    fn encode_start_args_supports_resources() {
        let signature = AbiSignature::new(vec![AbiParam::Scalar(AbiScalarType::I32)], Vec::new());
        let builder = ProcessBuilder::new("module", "proc")
            .signature(signature)
            .arg_resource(7u64);
        let bytes = encode_start_args(builder).expect("encode");
        let start = decode_rkyv::<ProcessStart>(&bytes).expect("decode");
        assert_eq!(start.entrypoint.args, [EntrypointArg::Resource(7)]);
    }

    #[test]
    fn encode_start_args_supports_shared_resources() {
        let signature = AbiSignature::new(vec![AbiParam::Scalar(AbiScalarType::U64)], Vec::new());
        let builder = ProcessBuilder::new("module", "proc")
            .signature(signature)
            .arg_resource(7u64);
        let bytes = encode_start_args(builder).expect("encode");
        let start = decode_rkyv::<ProcessStart>(&bytes).expect("decode");
        assert_eq!(start.entrypoint.args, [EntrypointArg::Resource(7)]);
    }

    #[test]
    fn encode_start_args_includes_network_grants() {
        let builder = ProcessBuilder::new("module", "proc")
            .network_egress_profile("egress.default")
            .network_ingress_binding("ingress.public");
        let bytes = encode_start_args(builder).expect("encode");
        let start = decode_rkyv::<ProcessStart>(&bytes).expect("decode");
        assert_eq!(start.network_egress_profiles, ["egress.default"]);
        assert_eq!(start.network_ingress_bindings, ["ingress.public"]);
    }

    #[test]
    fn encode_start_args_includes_storage_grants() {
        let builder = ProcessBuilder::new("module", "proc")
            .storage_log("control-plane.log")
            .storage_blob("control-plane.snapshots");
        let bytes = encode_start_args(builder).expect("encode");
        let start = decode_rkyv::<ProcessStart>(&bytes).expect("decode");
        assert_eq!(start.storage_logs, ["control-plane.log"]);
        assert_eq!(start.storage_blobs, ["control-plane.snapshots"]);
    }
}
