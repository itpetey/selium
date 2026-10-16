# Spec Delta

## REMOVED Requirements

### Requirement: Registration-Gated Readiness
**Reason**: Readiness gating is host policy that only helps the first boot. The runtime no longer verifies a guest's registration before admitting it as ready; consumers observe registration reactively (see `route-watch`) and converge on providers themselves, and restart recovery is the supervisor's concern.

**Migration**: Replace readiness-dependent startup with a route watch (or retry) on the provider's route. `mark_ready()` is deleted; a guest that wants to advertise health publishes a health record instead. A consumer that previously relied on a dependency being ready at boot now converges when the provider's registration appears.
