//! Windows service entry points.
//!
//! `kova --service` is the database. `kova-setup --service-iggy` runs the
//! bundled iggy-server and stops it when the service stops. Kova does not
//! start iggy itself.

use std::ffi::OsString;
use std::process::Command;
use std::sync::mpsc;
use std::time::Duration;

use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::{define_windows_service, service_dispatcher};

const KOVA: &str = "Kova";
const IGGY: &str = "KovaIggy";

pub fn run_kova() -> Result<(), String> {
    service_dispatcher::start(KOVA, ffi_kova).map_err(|err| err.to_string())
}

pub fn run_iggy() -> Result<(), String> {
    service_dispatcher::start(IGGY, ffi_iggy).map_err(|err| err.to_string())
}

define_windows_service!(ffi_kova, kova_main);
define_windows_service!(ffi_iggy, iggy_main);

fn kova_main(_arguments: Vec<OsString>) {
    if run_until_stop(KOVA, || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(crate::serve());
    })
    .is_err()
    {
        std::process::exit(1);
    }
}

fn iggy_main(_arguments: Vec<OsString>) {
    let config = std::env::var("KOVA_CONFIG").unwrap_or_else(|_| "kova.toml".into());
    let py = std::env::var("KOVA_IGGY_CONFIG_PY")
        .unwrap_or_else(|_| "packaging/iggy-server-config.py".into());
    let bin = std::env::var("KOVA_IGGY_BIN").unwrap_or_else(|_| "iggy-server.exe".into());
    if run_until_stop(IGGY, move || {
        let script = Command::new("python").arg(&py).arg(&config).output();
        if let Ok(output) = script {
            for line in String::from_utf8_lossy(&output.stdout).lines() {
                let Some(rest) = line.strip_prefix("export ") else {
                    continue;
                };
                let Some((key, value)) = rest.split_once('=') else {
                    continue;
                };
                let value = value.trim_matches('\'');
                std::env::set_var(key, value);
            }
        }
        let mut child = Command::new(&bin).spawn().expect("iggy-server");
        let _ = child.wait();
    })
    .is_err()
    {
        std::process::exit(1);
    }
}

fn run_until_stop(name: &str, work: impl FnOnce() + Send + 'static) -> Result<(), String> {
    let (tx, rx) = mpsc::channel();
    let handler = move |event| match event {
        ServiceControl::Stop | ServiceControl::Shutdown => {
            let _ = tx.send(());
            ServiceControlHandlerResult::NoError
        }
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        _ => ServiceControlHandlerResult::NotImplemented,
    };
    let status = service_control_handler::register(name, handler).map_err(|err| err.to_string())?;
    status
        .set_service_status(status_of(ServiceState::Running, ServiceControlAccept::STOP))
        .map_err(|err| err.to_string())?;
    let worker = std::thread::spawn(work);
    let _ = rx.recv();
    status
        .set_service_status(status_of(
            ServiceState::Stopped,
            ServiceControlAccept::empty(),
        ))
        .ok();
    // The listener has no shutdown hook. Ending the process is the stop.
    // Committed writes are in the WAL and replay on the next start.
    drop(worker);
    std::process::exit(0);
}

fn status_of(state: ServiceState, controls: ServiceControlAccept) -> ServiceStatus {
    ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted: controls,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    }
}
