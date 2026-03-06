#![allow(unused)]

use selium_guest_macros::entrypoint;

#[entrypoint]
async fn guest() -> Result<(), ()> {
    Ok(())
}

fn main() {}
