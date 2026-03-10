use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};

use selium_control_plane_api::{
    CanonicalCodec, ReferenceCodec, SuiteCase, Value, canonical_contract_fixture_suite,
    run_fixture_suite,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct PythonEncodeResponse {
    ok: bool,
    bytes: Option<Vec<u8>>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PythonDecodeResponse {
    ok: bool,
    value: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct PythonCodec {
    script: PathBuf,
}

impl PythonCodec {
    fn new() -> Self {
        Self {
            script: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join("python_contract_codec.py"),
        }
    }

    fn request(&self, payload: serde_json::Value) -> serde_json::Value {
        let request = serde_json::to_vec(&payload).expect("serialize python codec request");
        let mut child = Command::new("python3")
            .arg(&self.script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn python3 for codec proof");

        child
            .stdin
            .as_mut()
            .expect("python stdin")
            .write_all(&request)
            .expect("write python request");

        let output = child.wait_with_output().expect("wait for python codec");
        if !output.status.success() {
            panic!(
                "python codec process failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        serde_json::from_slice(&output.stdout).expect("parse python codec response")
    }
}

impl CanonicalCodec for PythonCodec {
    fn encode(
        &self,
        schema: &selium_control_plane_api::FixtureSchema,
        root: &selium_control_plane_api::TypeRef,
        value: &Value,
    ) -> Result<Vec<u8>, String> {
        let response: PythonEncodeResponse = serde_json::from_value(self.request(json!({
            "action": "encode",
            "schema": schema,
            "root": root,
            "value": value,
        })))
        .map_err(|err| err.to_string())?;

        match (response.ok, response.bytes, response.error) {
            (true, Some(bytes), _) => Ok(bytes),
            (false, _, Some(error)) => Err(error),
            _ => Err("malformed encode response from python codec".to_string()),
        }
    }

    fn decode(
        &self,
        schema: &selium_control_plane_api::FixtureSchema,
        root: &selium_control_plane_api::TypeRef,
        bytes: &[u8],
    ) -> Result<Value, String> {
        let response: PythonDecodeResponse = serde_json::from_value(self.request(json!({
            "action": "decode",
            "schema": schema,
            "root": root,
            "bytes": bytes,
        })))
        .map_err(|err| err.to_string())?;

        match (response.ok, response.value, response.error) {
            (true, Some(value), _) => Ok(value),
            (false, _, Some(error)) => Err(error),
            _ => Err("malformed decode response from python codec".to_string()),
        }
    }
}

#[test]
fn python_codec_passes_shared_fixture_suite() {
    let report = run_fixture_suite(&PythonCodec::new(), &canonical_contract_fixture_suite());
    report
        .into_result()
        .expect("python codec should pass the shared fixture suite");
}

#[test]
fn python_and_rust_reference_codec_interoperate_on_nested_fixture() {
    let python = PythonCodec::new();
    let rust = ReferenceCodec;
    let case = canonical_contract_fixture_suite()
        .into_iter()
        .find_map(|case| match case {
            SuiteCase::Golden(case) if case.name == "golden.nested-envelope-v1" => Some(case),
            _ => None,
        })
        .expect("nested envelope golden case");

    let python_bytes = python
        .encode(&case.schema, &case.root, &case.value)
        .expect("python encode");
    let rust_bytes = rust
        .encode(&case.schema, &case.root, &case.value)
        .expect("rust reference encode");
    assert_eq!(
        python_bytes, rust_bytes,
        "Python and Rust should emit the same canonical bytes"
    );

    let python_value = python
        .decode(&case.schema, &case.root, &rust_bytes)
        .expect("python decode rust bytes");
    let rust_value = rust
        .decode(&case.schema, &case.root, &python_bytes)
        .expect("rust decode python bytes");

    assert_eq!(
        python_value, case.value,
        "Python should decode Rust bytes to the same value"
    );
    assert_eq!(
        rust_value, case.value,
        "Rust should decode Python bytes to the same value"
    );
}
