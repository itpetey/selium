use selium_guest::{EntrypointMetadata, entrypoint};

#[derive(Debug)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

#[entrypoint]
async fn result_entrypoint() -> Result<(), TestError> {
    tracing::info!("result entrypoint invoked");
    Ok(())
}

#[test]
fn entrypoint_with_result_generates_metadata_and_returns_i32() {
    let entrypoint_metadata: EntrypointMetadata = result_entrypoint_entrypoint_metadata();
    assert_eq!(entrypoint_metadata.name, "result_entrypoint");

    // Verify the extern "C" fn exists and returns i32 = 0 on Ok(())
    let result = __selium_guest_entrypoint_result_entrypoint();
    assert_eq!(result, 0);
}
