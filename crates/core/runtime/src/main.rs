use std::{fs, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use selium_encoding::FlatMsg;
use selium_kernel::Kernel;
use selium_runtime::{ReadinessCondition, Runtime, RuntimeConfig, SystemGuestDescriptor};

#[derive(Debug, Clone)]
struct AppDef {
    name: String,
    path: PathBuf,
    entrypoint: String,
    module_id: String,
    dependencies: Vec<String>,
    tenant: Option<String>,
    readiness: String,
}

/// Selium runtime — loads and executes WebAssembly system guests.
#[derive(Parser)]
struct Cli {
    /// Start the discovery subsystem
    #[arg(long, default_value_t = false)]
    start_discovery: bool,

    /// One or more app definitions.
    ///
    /// Format: name=NAME,path=WASM[,entrypoint=FN][,module-id=ID][,dependencies=A,B][,tenant=T][,readiness=COND]
    ///
    /// Required keys: name, path.
    /// Defaults: entrypoint="main", module-id=<name>, readiness="immediate".
    #[arg(long, value_parser = parse_app)]
    app: Vec<AppDef>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let guests: Vec<SystemGuestDescriptor> = cli
        .app
        .iter()
        .map(|app| {
            let readiness = if app.readiness == "immediate" {
                ReadinessCondition::Immediate
            } else {
                ReadinessCondition::ActivityLogContains(app.readiness.clone())
            };

            Ok(SystemGuestDescriptor {
                name: app.name.clone(),
                module_id: app.module_id.clone(),
                module_bytes: fs::read(&app.path)?,
                entrypoint: app.entrypoint.clone(),
                arguments: Vec::new(),
                grants: Vec::new(),
                dependencies: app.dependencies.clone(),
                readiness,
                tenant: app.tenant.clone(),
            })
        })
        .collect::<Result<_>>()?;

    let kernel = Kernel::default();
    let runtime = Runtime::new(kernel);

    let config = RuntimeConfig {
        start_discovery: cli.start_discovery,
        system_guests: guests,
    };
    let report = runtime.bootstrap_system_guests(config)?;

    // Print activity log events
    let activity = runtime.activity_log();
    println!("=== Activity Log ===");
    for event in &activity {
        println!(
            "  [{:?}] {}: {}",
            event.kind,
            event.process_id.unwrap_or(0),
            event.message
        );
    }

    // Drain and print guest log messages for each bootstrapped guest
    for guest_report in &report.guests {
        println!("\n=== Guest Logs ({}) ===", guest_report.process_id);
        let frames = runtime
            .kernel()
            .processes()
            .drain_log_channel(guest_report.process_id)
            .map_err(|error| anyhow::anyhow!("drain log channel: {error}"))?;
        for frame in &frames {
            let record = selium_encoding::log::LogRecord::decode(frame)
                .map_err(|error| anyhow::anyhow!("decode log record: {error}"))?;
            println!("  {}", record.message);
        }
    }

    // Stop all processes and clean up
    println!("\n--- stopping processes ---");
    for guest_report in &report.guests {
        runtime.stop_process(guest_report.process_id)?;
    }
    if runtime.loaded_guest_count() != 0 {
        anyhow::bail!("expected 0 loaded guests after stop");
    }

    Ok(())
}

fn parse_app(raw: &str) -> Result<AppDef, String> {
    // Stateful split: a comma-separated segment that contains '=' starts
    // a new key; segments without '=' are appended to the previous key's
    // value (e.g. dependencies=a,b).
    let mut kvs: Vec<(String, String)> = Vec::new();
    for part in raw.split(',') {
        if let Some(eq) = part.find('=') {
            let key = part[..eq].trim().to_owned();
            let val = part[eq + 1..].trim().to_owned();
            if key.is_empty() {
                return Err(format!("empty key in '{}'", raw));
            }
            kvs.push((key, val));
        } else if let Some((_, last_val)) = kvs.last_mut() {
            last_val.push(',');
            last_val.push_str(part);
        } else {
            return Err(format!(
                "expected key=value pair, got '{}' in '{}'",
                part, raw
            ));
        }
    }

    let mut name = None;
    let mut path = None;
    let mut entrypoint = "main".to_owned();
    let mut module_id = None;
    let mut dependencies = Vec::new();
    let mut tenant = None;
    let mut readiness = "immediate".to_owned();

    for (key, val) in &kvs {
        match key.as_str() {
            "name" => {
                if val.is_empty() {
                    return Err("name must not be empty".into());
                }
                name = Some(val.clone());
            }
            "path" => {
                if val.is_empty() {
                    return Err("path must not be empty".into());
                }
                path = Some(PathBuf::from(val));
            }
            "entrypoint" => entrypoint = val.clone(),
            "module-id" => module_id = Some(val.clone()),
            "dependencies" => {
                dependencies = val.split(',').map(|s| s.trim().to_owned()).collect();
            }
            "tenant" => {
                tenant = if val.is_empty() {
                    None
                } else {
                    Some(val.clone())
                }
            }
            "readiness" => readiness = val.clone(),
            unknown => return Err(format!("unknown key '{}'", unknown)),
        }
    }

    let name = name.ok_or_else(|| format!("missing required key 'name' in '{}'", raw))?;
    let path = path.ok_or_else(|| format!("missing required key 'path' in '{}'", raw))?;
    let module_id = module_id.unwrap_or_else(|| name.clone());

    Ok(AppDef {
        name,
        path,
        entrypoint,
        module_id,
        dependencies,
        tenant,
        readiness,
    })
}
