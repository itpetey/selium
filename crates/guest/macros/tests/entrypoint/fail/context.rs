#![allow(unused)]

use selium_guest_macros::entrypoint;

struct Context;

#[entrypoint]
async fn guest(ctx: Context) -> Result<(), ()> {
    let _ = ctx;
    Ok(())
}

fn main() {}
