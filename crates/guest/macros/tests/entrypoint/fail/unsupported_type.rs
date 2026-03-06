#![allow(unused)]

use selium_guest_macros::entrypoint;

#[entrypoint]
fn guest((a, b): (i32, i32)) {}

fn main() {}
