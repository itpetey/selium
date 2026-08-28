use selium_guest::entrypoint;

#[entrypoint]
async fn unsupported_param_entrypoint(name: String) {}

fn main() {}
