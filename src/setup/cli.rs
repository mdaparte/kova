//! Terminal walkthrough and the window launcher.
//!
//! Both write the same `kova.toml`. A later run shows the values already
//! in the file.

use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::service::{self, ServiceHost, ServicePaths};
use super::{
    apply, create_root_user, find_up, graphical_session, resolve_security, security_open_confirmed,
    suggest, Answers, RootUser, Suggestion,
};

pub fn run() -> Result<(), String> {
    #[cfg(windows)]
    if std::env::args().any(|arg| arg == "--service-iggy") {
        return super::winsvc::run_iggy();
    }
    let mut window = false;
    let mut config: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--window" => window = true,
            "--config" => {
                let path = args
                    .next()
                    .ok_or_else(|| "--config needs a path".to_string())?;
                config = Some(PathBuf::from(path));
            }
            "--help" | "-h" => {
                println!("kova-setup [--window] [--config kova.toml]");
                println!("Opens the configuration again. Enter accepts the suggestion.");
                return Ok(());
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let config_path = config_file(config);
    let raw = fs_read(&config_path).unwrap_or_default();
    let mut suggestion = suggest(&raw)?;
    suggestion.service_installed = service_installed();
    let (answers, root, service) = if window {
        ask_window(&suggestion)?
    } else {
        ask_terminal(&suggestion)?
    };
    let text = apply(&raw, &answers)?;
    if let Some(parent) = config_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("{}: {err}", parent.display()))?;
        }
    }
    std::fs::write(&config_path, &text)
        .map_err(|err| format!("{}: {err}", config_path.display()))?;
    if answers.security_enabled {
        let bin = kova_bin()?;
        match ensure_root(&bin, &config_path, answers.port, &root.0, &root.1)? {
            RootUser::Created => {
                println!("Root user {} is in the catalog.", root.0);
                println!("Password: {}", root.1);
            }
            RootUser::AlreadyThere => {
                println!("An account is already in the catalog. The new password was not used.");
            }
        }
    }
    let host = host_for();
    let paths = service_paths(&config_path, answers.run_iggy)?;
    service::apply_choice(service, &host, &paths, true)?;
    if service {
        println!("Kova starts with the operating system.");
    }
    println!("Wrote {}", config_path.display());
    Ok(())
}

struct RootAccount(String, String);

fn ask_terminal(suggestion: &Suggestion) -> Result<(Answers, RootAccount, bool), String> {
    super::banner::print_banner();
    println!("Enter accepts the suggestion. Run this again to change it.");
    let port = prompt_u16("Listen port", suggestion.port)?;
    let data_dir = prompt("Data directory", &suggestion.data_dir);
    let run_iggy = prompt_yes("Run the bundled iggy-server?", suggestion.run_iggy);
    let (iggy_user, iggy_password) = if run_iggy {
        let user = prompt("Iggy user", &suggestion.iggy_user);
        let password = if suggestion.keep_iggy_password {
            let typed = prompt("Iggy password", "keep");
            if typed == "keep" {
                None
            } else {
                Some(typed)
            }
        } else {
            Some(prompt("Iggy password", &suggestion.iggy_password))
        };
        (user, password)
    } else {
        (String::new(), None)
    };
    let security_enabled = ask_security(suggestion.security_enabled)?;
    let root = if security_enabled {
        let user = prompt("Root user", &suggestion.root_user);
        let password = prompt("Root password", &suggestion.root_password);
        RootAccount(user, password)
    } else {
        RootAccount(String::new(), String::new())
    };
    let service = prompt_yes(
        "Start with the operating system?",
        suggestion.service_installed,
    );
    let log_target = prompt("Log target (stderr or a file path)", &suggestion.log_target);
    let (log_rotate, log_max_bytes, log_keep) = if log_target.trim() == "stderr" {
        (
            "none".to_string(),
            suggestion.log_max_bytes,
            suggestion.log_keep,
        )
    } else {
        let rotate = prompt(
            "Log rotation (none, date, size, date-and-size)",
            &suggestion.log_rotate,
        );
        let max_bytes = prompt_u64("Log max bytes", suggestion.log_max_bytes)?;
        let keep = prompt_u64("Log files to keep", suggestion.log_keep)?;
        (rotate, max_bytes, keep)
    };
    Ok((
        Answers {
            port,
            data_dir,
            run_iggy,
            iggy_user,
            iggy_password,
            security_enabled,
            log_target,
            log_rotate,
            log_max_bytes,
            log_keep,
        },
        root,
        service,
    ))
}

fn ask_security(suggested_closed: bool) -> Result<bool, String> {
    if suggested_closed {
        println!("Security is closed. Enter keeps it closed.");
        let typed = prompt_raw("Type open to leave every command unauthenticated");
        if typed.is_empty() {
            return Ok(true);
        }
        if typed.eq_ignore_ascii_case("open") {
            let confirmation = prompt_raw("Type OPEN to confirm security stays off");
            return resolve_security(true, true, &confirmation);
        }
        return Err(format!("unrecognized answer {typed:?}"));
    }
    println!("Security is open. Enter keeps it open.");
    let typed = prompt_raw("Type closed to require a user");
    if typed.is_empty() {
        return Ok(false);
    }
    if typed.eq_ignore_ascii_case("closed") || typed.eq_ignore_ascii_case("close") {
        return Ok(true);
    }
    if security_open_confirmed(&typed) {
        return Ok(false);
    }
    Err(format!("unrecognized answer {typed:?}"))
}

fn spawn_window(script: &Path) -> Result<std::process::Child, String> {
    let mut last = String::from("python3 was not found");
    for program in ["python3", "python"] {
        match Command::new(program)
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
        {
            Ok(child) => return Ok(child),
            Err(err) => last = format!("{program}: {err}"),
        }
    }
    Err(last)
}

fn ask_window(suggestion: &Suggestion) -> Result<(Answers, RootAccount, bool), String> {
    if !graphical_session() {
        return Err("no graphical session. Run kova-setup without --window".into());
    }
    let script = std::env::current_exe()
        .ok()
        .and_then(|exe| find_up(&exe, "packaging/kova-setup-window.py"))
        .or_else(|| find_up(Path::new("."), "packaging/kova-setup-window.py"))
        .ok_or_else(|| "packaging/kova-setup-window.py was not found".to_string())?;
    let input = serde_json::to_string(suggestion).map_err(|err| err.to_string())?;
    let mut child = spawn_window(&script)?;
    child
        .stdin
        .take()
        .ok_or_else(|| "window stdin".to_string())?
        .write_all(input.as_bytes())
        .map_err(|err| err.to_string())?;
    let output = child
        .wait_with_output()
        .map_err(|err| format!("window: {err}"))?;
    if !output.status.success() {
        return Err("the window closed without saving".into());
    }
    let form: WindowOut =
        serde_json::from_slice(&output.stdout).map_err(|err| format!("window reply: {err}"))?;
    let security_enabled = resolve_security(
        suggestion.security_enabled,
        form.leave_open,
        &form.confirmation,
    )?;
    let iggy_password = if form.keep_iggy_password && form.iggy_password.is_empty() {
        None
    } else if form.run_iggy {
        Some(form.iggy_password)
    } else {
        None
    };
    Ok((
        Answers {
            port: form.port,
            data_dir: form.data_dir,
            run_iggy: form.run_iggy,
            iggy_user: form.iggy_user,
            iggy_password,
            security_enabled,
            log_target: form.log_target,
            log_rotate: form.log_rotate,
            log_max_bytes: form.log_max_bytes,
            log_keep: form.log_keep,
        },
        RootAccount(form.root_user, form.root_password),
        form.service,
    ))
}

#[derive(serde::Deserialize)]
struct WindowOut {
    port: u16,
    data_dir: String,
    run_iggy: bool,
    iggy_user: String,
    iggy_password: String,
    keep_iggy_password: bool,
    leave_open: bool,
    confirmation: String,
    root_user: String,
    root_password: String,
    service: bool,
    log_target: String,
    log_rotate: String,
    log_max_bytes: u64,
    log_keep: u64,
}

fn ensure_root(
    bin: &Path,
    config: &Path,
    port: u16,
    user: &str,
    password: &str,
) -> Result<RootUser, String> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    if !super::port_free(port) {
        return create_root_user(addr, user, password);
    }
    let mut child = Command::new(bin)
        .env("KOVA_CONFIG", config)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| format!("start {}: {err}", bin.display()))?;
    let result = if wait_port(port, Duration::from_secs(30)) {
        create_root_user(addr, user, password)
    } else {
        Err(format!("kova did not listen on {port}"))
    };
    let _ = child.kill();
    let _ = child.wait();
    result
}

fn wait_port(port: u16, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if std::net::TcpStream::connect_timeout(
            &SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

fn service_paths(config: &Path, run_iggy: bool) -> Result<ServicePaths, String> {
    let kova = kova_bin()?;
    let iggy_name = if cfg!(windows) {
        "iggy-server.exe"
    } else {
        "iggy-server"
    };
    let sibling = kova.with_file_name(iggy_name);
    let iggy_bin = if sibling.is_file() {
        sibling
    } else {
        find_up(&kova, "target/iggy-server").unwrap_or(kova.with_file_name(iggy_name))
    };
    let iggy_config_py = find_up(&kova, "packaging/iggy-server-config.py")
        .or_else(|| find_up(Path::new("."), "packaging/iggy-server-config.py"))
        .unwrap_or_else(|| PathBuf::from("packaging/iggy-server-config.py"));
    Ok(ServicePaths {
        kova_bin: kova,
        iggy_bin,
        iggy_config_py,
        config_file: std::fs::canonicalize(config).unwrap_or_else(|_| config.to_path_buf()),
        run_iggy,
    })
}

fn kova_bin() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|err| err.to_string())?;
    let name = if cfg!(windows) { "kova.exe" } else { "kova" };
    let sibling = exe
        .parent()
        .ok_or_else(|| "setup has no directory".to_string())?
        .join(name);
    if sibling.is_file() {
        return Ok(sibling);
    }
    Err(format!("kova binary not found next to {}", exe.display()))
}

fn host_for() -> ServiceHost {
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        if is_root() {
            return ServiceHost::LaunchDaemon {
                dir: PathBuf::from("/Library/LaunchDaemons"),
            };
        }
        return ServiceHost::LaunchAgent {
            dir: home.join("Library").join("LaunchAgents"),
        };
    }
    #[cfg(target_os = "windows")]
    {
        let dir = std::env::var_os("ProgramData")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        return ServiceHost::Windows {
            dir: dir.join("Kova"),
        };
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if is_root() {
            return ServiceHost::SystemdSystem {
                unit_dir: PathBuf::from("/etc/systemd/system"),
            };
        }
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
            .unwrap_or_else(|| PathBuf::from(".config"));
        ServiceHost::SystemdUser {
            unit_dir: base.join("systemd").join("user"),
        }
    }
}

fn is_root() -> bool {
    #[cfg(unix)]
    {
        std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|text| text.trim() == "0")
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn config_file(flag: Option<PathBuf>) -> PathBuf {
    if let Some(path) = flag {
        return path;
    }
    if let Ok(path) = std::env::var("KOVA_CONFIG") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    PathBuf::from(crate::config::CONFIG_FILE)
}

fn fs_read(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

fn prompt(label: &str, suggestion: &str) -> String {
    let typed = prompt_raw(&format!("{label} [{suggestion}]"));
    if typed.is_empty() {
        suggestion.to_string()
    } else {
        typed
    }
}

fn prompt_raw(label: &str) -> String {
    print!("{label}: ");
    let _ = io::stdout().flush();
    let mut line = String::new();
    let _ = io::stdin().lock().read_line(&mut line);
    line.trim().to_string()
}

fn prompt_yes(label: &str, default_yes: bool) -> bool {
    let hint = if default_yes { "yes" } else { "no" };
    matches!(
        prompt(label, hint).to_ascii_lowercase().as_str(),
        "y" | "yes"
    )
}

fn prompt_u16(label: &str, suggestion: u16) -> Result<u16, String> {
    let typed = prompt(label, &suggestion.to_string());
    typed
        .parse::<u16>()
        .map_err(|_| format!("{label} {typed:?} is not a port"))
}

fn prompt_u64(label: &str, suggestion: u64) -> Result<u64, String> {
    let typed = prompt(label, &suggestion.to_string());
    typed
        .parse::<u64>()
        .map_err(|_| format!("{label} {typed:?} is not a number"))
}

fn service_installed() -> bool {
    match host_for() {
        ServiceHost::SystemdUser { unit_dir } | ServiceHost::SystemdSystem { unit_dir } => {
            unit_dir.join("kova.service").is_file()
        }
        ServiceHost::Windows { dir } => dir.join("install-kova-service.ps1").is_file(),
        ServiceHost::LaunchAgent { dir } | ServiceHost::LaunchDaemon { dir } => {
            dir.join("com.kova.server.plist").is_file()
        }
    }
}
