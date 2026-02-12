use selium_userland::DependencyId;
use selium_userland_macros::dependency_id;

#[test]
fn dependency_id_matches_blake3_prefix() {
    let value: DependencyId = dependency_id!("selium.clock");
    let hash = blake3::hash("selium.clock".as_bytes());
    let mut expected = [0u8; 16];
    expected.copy_from_slice(&hash.as_bytes()[..16]);
    assert_eq!(value.0, expected);
}
