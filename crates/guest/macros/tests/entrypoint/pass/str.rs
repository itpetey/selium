use selium_guest_macros::entrypoint;

#[entrypoint]
fn takes_str(domain: &str) {
    let _ = domain.len();
}

fn main() {}
