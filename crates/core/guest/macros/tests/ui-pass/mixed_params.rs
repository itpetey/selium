use selium_guest::entrypoint;

#[entrypoint]
async fn mixed_entrypoint(_app_id: u32, _generation: u64, _flags: u8, _code: i32) {}

fn main() {}
