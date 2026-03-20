//! Routing guest - HTTP proxy and load balancing.
//!
//! Provides:
//! - Network listener
//! - HTTP proxy logic
//! - Round-robin load balancing
//! - Circuit breaker

use selium_guest_runtime::{Attribution, GuestResult, RpcCall, RpcServer};
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct Backend {
    pub address: String,
    pub healthy: bool,
    pub failures: u32,
}

impl Backend {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            healthy: true,
            failures: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CircuitState {
    #[default]
    Closed,
    Open,
    HalfOpen,
}

pub struct CircuitBreaker {
    state: CircuitState,
    failures: u32,
    threshold: u32,
    #[allow(dead_code)]
    half_open_attempts: usize,
}

impl CircuitBreaker {
    pub fn new(threshold: u32) -> Self {
        Self {
            state: CircuitState::Closed,
            failures: 0,
            threshold,
            half_open_attempts: 0,
        }
    }

    pub fn record_failure(&mut self) {
        self.failures += 1;
        if self.failures >= self.threshold {
            self.state = CircuitState::Open;
        }
    }

    pub fn record_success(&mut self) {
        self.failures = 0;
        if self.state == CircuitState::HalfOpen {
            self.state = CircuitState::Closed;
        }
    }

    pub fn is_open(&self) -> bool {
        self.state == CircuitState::Open
    }
}

pub struct RoutingGuest {
    backends: VecDeque<Backend>,
    current_index: usize,
    circuit_breaker: CircuitBreaker,
    #[allow(dead_code)]
    server: RpcServer,
}

#[allow(dead_code)]
impl RoutingGuest {
    pub fn new() -> Self {
        let server = RpcServer::new();
        Self {
            backends: VecDeque::new(),
            current_index: 0,
            circuit_breaker: CircuitBreaker::new(5),
            server,
        }
    }

    pub fn add_backend(&mut self, backend: Backend) {
        self.backends.push_back(backend);
    }

    pub fn next_backend(&mut self) -> Option<&Backend> {
        if self.circuit_breaker.is_open() {
            return None;
        }

        let healthy_backends: Vec<_> = self.backends.iter().filter(|b| b.healthy).collect();

        if healthy_backends.is_empty() {
            return None;
        }

        if self.current_index >= healthy_backends.len() {
            self.current_index = 0;
        }

        let backend = healthy_backends[self.current_index];
        self.current_index += 1;
        Some(backend)
    }

    pub fn mark_backend_failed(&mut self, address: &str) {
        for backend in &mut self.backends {
            if backend.address == address {
                backend.failures += 1;
                if backend.failures >= 3 {
                    backend.healthy = false;
                }
                self.circuit_breaker.record_failure();
                break;
            }
        }
    }

    pub fn circuit_state(&self) -> CircuitState {
        self.circuit_breaker.state
    }

    #[allow(unused_variables)]
    fn handle_route(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }
}

impl Default for RoutingGuest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routing_guest_new() {
        let guest = RoutingGuest::new();
        assert!(guest.backends.is_empty());
        assert_eq!(guest.circuit_state(), CircuitState::Closed);
    }

    #[test]
    fn test_add_backend() {
        let mut guest = RoutingGuest::new();
        guest.add_backend(Backend::new("localhost:8080"));
        assert_eq!(guest.backends.len(), 1);
    }

    #[test]
    fn test_round_robin() {
        let mut guest = RoutingGuest::new();
        guest.add_backend(Backend::new("localhost:8080"));
        guest.add_backend(Backend::new("localhost:8081"));

        let first = guest.next_backend().map(|b| b.address.clone());
        let second = guest.next_backend().map(|b| b.address.clone());
        let third = guest.next_backend().map(|b| b.address.clone());

        assert_eq!(first, Some("localhost:8080".to_string()));
        assert_eq!(second, Some("localhost:8081".to_string()));
        assert_eq!(third, Some("localhost:8080".to_string()));
    }

    #[test]
    fn test_circuit_breaker_opens() {
        let mut cb = CircuitBreaker::new(3);
        cb.record_failure();
        cb.record_failure();
        assert!(!cb.is_open());
        cb.record_failure();
        assert!(cb.is_open());
    }
}
