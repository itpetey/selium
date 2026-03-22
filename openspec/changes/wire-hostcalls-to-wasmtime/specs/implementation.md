This change implements requirements from existing specs - no new or modified capability requirements.

## Implementation of Existing Requirements

This change brings the implementation into compliance with these existing specs:

- **host-kernel/spec.md**: "Host executes WASM guests" - requires host to link capability functions
- **hostcall-abi/spec.md**: ABI definitions - requires functions to be wired for guest access
- **time-hostcall/spec.md**: Time primitives
- **storage-hostcall/spec.md**: Storage operations
- **network-hostcall/spec.md**: Network operations  
- **queue-hostcall/spec.md**: Queue operations

The implementation currently exists but is not wired to the wasmtime linker - this change completes the wiring.