use selium_guest::entrypoint;

#[entrypoint]
async fn pointer_entrypoint(_resolver: (u64, u64)) {}

fn main() {}
