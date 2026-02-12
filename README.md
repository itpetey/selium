# Selium

[![Crates.io][crates-badge]][crates-url]
[![MPL2 licensed][mpl-badge]][mpl-url]
[![Build Status][build-badge]][build-url]
[![Audit Status][audit-badge]][audit-url]

[crates-badge]: https://img.shields.io/crates/v/selium-kernel.svg
[crates-url]: https://crates.io/crates/selium-kernel
[mpl-badge]: https://img.shields.io/badge/licence-MPL2-blue.svg
[mpl-url]: https://github.com/seliumlabs/selium/blob/main/LICENCE
[build-badge]: https://github.com/seliumlabs/selium/actions/workflows/ci.yml/badge.svg
[build-url]: https://github.com/seliumlabs/selium/actions/workflows/ci.yml
[audit-badge]: https://github.com/seliumlabs/selium/actions/workflows/audit.yml/badge.svg
[audit-url]: https://github.com/seliumlabs/selium/actions/workflows/audit.yml

Selium is a software framework and runtime for building scalable, connected applications. With Selium, you can compose entire application stacks with **strict capabilities**, **typed I/O**, and **zero infrastructure**.

You get the ergonomics of an application platform, with the safety properties of a capability system:

- **Zero DevOps**: a batteries-included platform that is entirely composable in code.
- **Ergonomic I/O everywhere**: first class composable messaging, even when interacting with network protocols.
- **End-to-end type safety**: seamless type support across process and network boundaries.
- **No ambient authority**: guests only receive the capabilities you grant them at runtime.

**Status:** alpha (APIs and project structure are still evolving).

---

## Getting started

The fastest way to orient yourself is by [reading the docs](https://selium.com/docs).

If you'd prefer to learn by example, [check out those here](examples/).

You can also find [Selium modules in their own repo](https://github.com/seliumlabs/selium-modules). These modules add significant extra functionality on top of the core platform.

---

## Contributing

We'd love your help! Check out the issue log or join in a discussion to get started.

See `AGENTS.md` for contribution rules, including: stable Rust only, `tracing` logging, and International English.

---

## Licence

MPL v2 (see `LICENCE`)
