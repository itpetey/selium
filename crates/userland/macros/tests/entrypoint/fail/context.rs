#![allow(unused)]

use selium_userland_macros::entrypoint;

struct Context;

#[entrypoint]
async fn guest(ctx: Context) -> Result<(), ()> {
    let _ = ctx;
    Ok(())
}

fn main() {}
