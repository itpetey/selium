#![allow(unused)]

use anyhow::Result;
use selium_guest_macros::entrypoint;

#[entrypoint]
fn guest() -> Result<()> {
    Ok(())
}

fn main() {}
