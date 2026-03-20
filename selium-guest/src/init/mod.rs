//! Init guest - First guest to boot and orchestrate system services.
//!
//! Responsibilities:
//! - Spawn core services (consensus, scheduler, discovery, supervisor, routing)
//! - Read static configuration
//! - Wait for services to become ready
//! - Provide inter-service handles

use crate::error::GuestResult;
use crate::async_::{spawn, yield_now};

#[derive(Debug, Clone)]
pub struct ServiceHandle {
    pub name: String,
    pub guest_id: u64,
}

impl ServiceHandle {
    pub fn new(name: impl Into<String>, guest_id: u64) -> Self {
        Self {
            name: name.into(),
            guest_id,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceConfig {
    pub name: String,
    pub module: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InitConfig {
    pub services: Vec<ServiceConfig>,
}

impl Default for InitConfig {
    fn default() -> Self {
        Self {
            services: vec![
                ServiceConfig {
                    name: "consensus".to_string(),
                    module: "consensus.wasm".to_string(),
                    enabled: true,
                },
                ServiceConfig {
                    name: "scheduler".to_string(),
                    module: "scheduler.wasm".to_string(),
                    enabled: true,
                },
                ServiceConfig {
                    name: "discovery".to_string(),
                    module: "discovery.wasm".to_string(),
                    enabled: true,
                },
                ServiceConfig {
                    name: "supervisor".to_string(),
                    module: "supervisor.wasm".to_string(),
                    enabled: true,
                },
                ServiceConfig {
                    name: "routing".to_string(),
                    module: "routing.wasm".to_string(),
                    enabled: true,
                },
            ],
        }
    }
}

pub struct InitGuest {
    spawned_services: Vec<ServiceHandle>,
    config: InitConfig,
}

impl InitGuest {
    pub fn new() -> Self {
        Self {
            spawned_services: Vec::new(),
            config: InitConfig::default(),
        }
    }
}

impl Default for InitGuest {
    fn default() -> Self {
        Self::new()
    }
}

impl InitGuest {
    pub fn with_config(config: InitConfig) -> Self {
        Self {
            spawned_services: Vec::new(),
            config,
        }
    }

    pub fn register_service(&mut self, handle: ServiceHandle) {
        self.spawned_services.push(handle);
    }

    pub fn get_service(&self, name: &str) -> Option<&ServiceHandle> {
        self.spawned_services.iter().find(|s| s.name == name)
    }

    pub fn services(&self) -> &[ServiceHandle] {
        &self.spawned_services
    }

    pub fn config(&self) -> &InitConfig {
        &self.config
    }
}

pub async fn run_init() -> GuestResult {
    let init = InitGuest::new();
    
    let enabled_services: Vec<_> = init.config()
        .services
        .iter()
        .filter(|s| s.enabled)
        .collect();
    
    for service_config in enabled_services {
        let name = service_config.name.clone();
        spawn(async move {
            // Service spawning would go here
            let _ = name;
            GuestResult::Ok(())
        });
        
        yield_now().await;
    }
    
    GuestResult::Ok(())
}

pub fn create_init_module() -> GuestResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_guest_new() {
        let init = InitGuest::new();
        assert!(init.services().is_empty());
    }

    #[test]
    fn test_init_guest_with_config() {
        let config = InitConfig {
            services: vec![
                ServiceConfig {
                    name: "test".to_string(),
                    module: "test.wasm".to_string(),
                    enabled: true,
                },
            ],
        };
        let init = InitGuest::with_config(config);
        assert!(init.services().is_empty());
        assert_eq!(init.config().services.len(), 1);
    }

    #[test]
    fn test_register_service() {
        let mut init = InitGuest::new();
        init.register_service(ServiceHandle::new("consensus", 1));
        init.register_service(ServiceHandle::new("scheduler", 2));
        
        assert_eq!(init.services().len(), 2);
    }

    #[test]
    fn test_get_service() {
        let mut init = InitGuest::new();
        init.register_service(ServiceHandle::new("consensus", 1));
        init.register_service(ServiceHandle::new("scheduler", 2));
        
        let consensus = init.get_service("consensus");
        assert!(consensus.is_some());
        assert_eq!(consensus.unwrap().guest_id, 1);
        
        let unknown = init.get_service("unknown");
        assert!(unknown.is_none());
    }

    #[test]
    fn test_default_init_config() {
        let config = InitConfig::default();
        assert_eq!(config.services.len(), 5);
        assert!(config.services.iter().all(|s| s.enabled));
    }
}
