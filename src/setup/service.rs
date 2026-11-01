//! Operating-system service files for Kova.
//!
//! Linux writes a systemd unit. Windows writes a service install script.
//! macOS writes a launchd plist. The engine does not supervise itself.
//! Removing the service deletes those files and leaves the data directory.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Binaries and the config file the unit will start.
#[derive(Clone)]
pub struct ServicePaths {
    pub kova_bin: PathBuf,
    pub iggy_bin: PathBuf,
    pub iggy_config_py: PathBuf,
    pub config_file: PathBuf,
    pub run_iggy: bool,
}

/// Where the unit files go.
#[derive(Clone)]
pub enum ServiceHost {
    /// `systemctl --user`. Starts at graphical or user login.
    SystemdUser { unit_dir: PathBuf },
    /// A system unit. Starts at boot.
    SystemdSystem { unit_dir: PathBuf },
    /// `sc.exe` install script.
    Windows { dir: PathBuf },
    /// `~/Library/LaunchAgents`.
    LaunchAgent { dir: PathBuf },
    /// `/Library/LaunchDaemons`.
    LaunchDaemon { dir: PathBuf },
}

#[derive(Clone)]
pub struct WrittenFile {
    pub path: PathBuf,
    pub body: String,
    pub executable: bool,
}

#[derive(Clone)]
pub struct ServicePlan {
    pub files: Vec<WrittenFile>,
    /// Commands that install and start, iggy before Kova.
    pub enable: Vec<String>,
    /// Commands that stop and uninstall, Kova before iggy.
    pub disable: Vec<String>,
}

pub fn plan(host: &ServiceHost, paths: &ServicePaths) -> ServicePlan {
    match host {
        ServiceHost::SystemdUser { unit_dir } | ServiceHost::SystemdSystem { unit_dir } => {
            systemd_plan(
                unit_dir,
                paths,
                matches!(host, ServiceHost::SystemdUser { .. }),
            )
        }
        ServiceHost::Windows { dir } => windows_plan(dir, paths),
        ServiceHost::LaunchAgent { dir } | ServiceHost::LaunchDaemon { dir } => {
            launchd_plan(dir, paths, matches!(host, ServiceHost::LaunchAgent { .. }))
        }
    }
}

pub fn write_plan(plan: &ServicePlan) -> Result<(), String> {
    for file in &plan.files {
        if let Some(parent) = file.path.parent() {
            fs::create_dir_all(parent).map_err(|err| format!("{}: {err}", parent.display()))?;
        }
        fs::write(&file.path, &file.body)
            .map_err(|err| format!("{}: {err}", file.path.display()))?;
        #[cfg(unix)]
        if file.executable {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&file.path)
                .map_err(|err| err.to_string())?
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&file.path, perms).map_err(|err| err.to_string())?;
        }
    }
    Ok(())
}

pub fn remove_plan(plan: &ServicePlan) -> Result<(), String> {
    for file in &plan.files {
        if file.path.exists() {
            fs::remove_file(&file.path).map_err(|err| format!("{}: {err}", file.path.display()))?;
        }
    }
    Ok(())
}

/// Writes the unit, or removes it. `run_hooks` runs systemctl, sc.exe, or launchctl.
pub fn apply_choice(
    want: bool,
    host: &ServiceHost,
    paths: &ServicePaths,
    run_hooks: bool,
) -> Result<(), String> {
    let plan = plan(host, paths);
    if want {
        write_plan(&plan)?;
        if run_hooks {
            run_commands(&plan.enable)?;
        }
        return Ok(());
    }
    let present = plan.files.iter().any(|file| file.path.exists());
    if present && run_hooks {
        let _ = run_commands(&plan.disable);
    }
    if present {
        remove_plan(&plan)?;
    }
    Ok(())
}

fn run_commands(commands: &[String]) -> Result<(), String> {
    for command in commands {
        let status = Command::new("sh")
            .arg("-c")
            .arg(command)
            .status()
            .map_err(|err| format!("{command}: {err}"))?;
        if !status.success() {
            return Err(format!("{command} exited with {status}"));
        }
    }
    Ok(())
}

fn systemd_plan(unit_dir: &Path, paths: &ServicePaths, user: bool) -> ServicePlan {
    let scope = if user { "--user " } else { "" };
    let target = if user {
        "default.target"
    } else {
        "multi-user.target"
    };
    let mut files = Vec::new();
    let mut enable = vec![format!("systemctl {scope}daemon-reload")];
    let mut disable = Vec::new();
    if paths.run_iggy {
        let script = unit_dir.join("kova-iggy.sh");
        files.push(WrittenFile {
            path: script.clone(),
            body: iggy_script(paths),
            executable: true,
        });
        files.push(WrittenFile {
            path: unit_dir.join("kova-iggy.service"),
            body: systemd_unit(
                "Kova bundled iggy-server",
                &quote_systemd(&script.display().to_string()),
                None,
                None,
                target,
                "",
            ),
            executable: false,
        });
        enable.push(format!("systemctl {scope}enable --now kova-iggy.service"));
        disable.push(format!("systemctl {scope}disable --now kova-iggy.service"));
    }
    let after = if paths.run_iggy {
        "\nAfter=kova-iggy.service\nRequires=kova-iggy.service"
    } else {
        ""
    };
    files.push(WrittenFile {
        path: unit_dir.join("kova.service"),
        body: systemd_unit(
            "Kova",
            &quote_systemd(&paths.kova_bin.display().to_string()),
            Some(&paths.config_file),
            paths.config_file.parent(),
            target,
            after,
        ),
        executable: false,
    });
    enable.push(format!("systemctl {scope}enable --now kova.service"));
    disable.insert(0, format!("systemctl {scope}disable --now kova.service"));
    ServicePlan {
        files,
        enable,
        disable,
    }
}

fn systemd_unit(
    description: &str,
    exec: &str,
    config: Option<&Path>,
    work: Option<&Path>,
    target: &str,
    extra_unit: &str,
) -> String {
    let mut body = format!("[Unit]\nDescription={description}\nAfter=network-online.target\nWants=network-online.target{extra_unit}\n\n[Service]\nType=simple\n");
    if let Some(config) = config {
        body.push_str(&format!(
            "Environment=\"KOVA_CONFIG={}\"\n",
            config.display()
        ));
    }
    if let Some(work) = work {
        if !work.as_os_str().is_empty() {
            body.push_str(&format!("WorkingDirectory={}\n", work.display()));
        }
    }
    body.push_str(&format!(
        "ExecStart={exec}\nRestart=on-failure\nRestartSec=1\n\n[Install]\nWantedBy={target}\n"
    ));
    body
}

fn quote_systemd(value: &str) -> String {
    if value.contains(' ') {
        format!("\"{value}\"")
    } else {
        value.to_string()
    }
}

fn iggy_script(paths: &ServicePaths) -> String {
    format!(
        "#!/bin/sh\nset -eu\neval \"$({} {})\"\nexec {}\n",
        sh_quote(&paths.iggy_config_py.display().to_string()),
        sh_quote(&paths.config_file.display().to_string()),
        sh_quote(&paths.iggy_bin.display().to_string())
    )
}

fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn windows_plan(dir: &Path, paths: &ServicePaths) -> ServicePlan {
    let config = paths.config_file.display().to_string().replace('\'', "");
    let mut body = String::from("$ErrorActionPreference = 'Stop'\n");
    if paths.run_iggy {
        let setup = paths.kova_bin.with_file_name(iggy_setup_name());
        body.push_str(&format!(
            "sc.exe create KovaIggy binPath= \"\\\"{}\\\" --service-iggy\" start= auto\n\
             sc.exe failure KovaIggy reset= 0 actions= restart/5000\n\
             sc.exe start KovaIggy\n",
            setup.display()
        ));
    }
    body.push_str(&format!(
        "sc.exe create Kova binPath= \"\\\"{}\\\" --service\" start= auto\n\
         sc.exe failure Kova reset= 0 actions= restart/5000\n\
         [Environment]::SetEnvironmentVariable('KOVA_CONFIG', '{config}', 'Machine')\n\
         sc.exe start Kova\n",
        paths.kova_bin.display()
    ));
    let install = dir.join("install-kova-service.ps1");
    let uninstall = dir.join("uninstall-kova-service.ps1");
    let mut remove = String::from("sc.exe stop Kova\nsc.exe delete Kova\n");
    if paths.run_iggy {
        remove.push_str("sc.exe stop KovaIggy\nsc.exe delete KovaIggy\n");
    }
    ServicePlan {
        files: vec![
            WrittenFile {
                path: install.clone(),
                body,
                executable: false,
            },
            WrittenFile {
                path: uninstall.clone(),
                body: remove,
                executable: false,
            },
        ],
        enable: vec![format!(
            "powershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"",
            install.display()
        )],
        disable: vec![format!(
            "powershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"",
            uninstall.display()
        )],
    }
}

fn iggy_setup_name() -> String {
    if cfg!(windows) {
        "kova-setup.exe".into()
    } else {
        "kova-setup".into()
    }
}

fn launchd_plan(dir: &Path, paths: &ServicePaths, agent: bool) -> ServicePlan {
    let domain = if agent { "gui/$UID" } else { "system" };
    let mut files = Vec::new();
    let mut enable = Vec::new();
    let mut disable = Vec::new();
    if paths.run_iggy {
        let script = dir.join("kova-iggy.sh");
        files.push(WrittenFile {
            path: script.clone(),
            body: iggy_script(paths),
            executable: true,
        });
        let plist = dir.join("com.kova.iggy.plist");
        files.push(WrittenFile {
            path: plist.clone(),
            body: launchd_plist(
                "com.kova.iggy",
                &[script.display().to_string()],
                None,
                paths.config_file.parent(),
            ),
            executable: false,
        });
        enable.push(format!(
            "launchctl bootstrap {domain} \"{}\"",
            plist.display()
        ));
        disable.push(format!("launchctl bootout {domain}/com.kova.iggy"));
    }
    let plist = dir.join("com.kova.server.plist");
    files.push(WrittenFile {
        path: plist.clone(),
        body: launchd_plist(
            "com.kova.server",
            &[paths.kova_bin.display().to_string()],
            Some(&paths.config_file),
            paths.config_file.parent(),
        ),
        executable: false,
    });
    enable.push(format!(
        "launchctl bootstrap {domain} \"{}\"",
        plist.display()
    ));
    disable.insert(0, format!("launchctl bootout {domain}/com.kova.server"));
    ServicePlan {
        files,
        enable,
        disable,
    }
}

fn launchd_plist(
    label: &str,
    args: &[String],
    config: Option<&Path>,
    work: Option<&Path>,
) -> String {
    let mut arguments = String::new();
    for arg in args {
        arguments.push_str(&format!("    <string>{}</string>\n", xml_escape(arg)));
    }
    let env = if let Some(config) = config {
        format!(
            "  <key>EnvironmentVariables</key>\n  <dict>\n    <key>KOVA_CONFIG</key>\n    <string>{}</string>\n  </dict>\n",
            xml_escape(&config.display().to_string())
        )
    } else {
        String::new()
    };
    let work = work
        .filter(|path| !path.as_os_str().is_empty())
        .map(|path| {
            format!(
                "  <key>WorkingDirectory</key>\n  <string>{}</string>\n",
                xml_escape(&path.display().to_string())
            )
        })
        .unwrap_or_default();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n<dict>\n\
           <key>Label</key>\n  <string>{label}</string>\n\
           <key>ProgramArguments</key>\n  <array>\n{arguments}  </array>\n\
         {env}{work}\
           <key>RunAtLoad</key>\n  <true/>\n\
           <key>KeepAlive</key>\n  <true/>\n\
         </dict>\n</plist>\n"
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(root: &Path, run_iggy: bool) -> ServicePaths {
        ServicePaths {
            kova_bin: root.join("bin").join("kova"),
            iggy_bin: root.join("bin").join("iggy-server"),
            iggy_config_py: root.join("packaging").join("iggy-server-config.py"),
            config_file: root.join("kova.toml"),
            run_iggy,
        }
    }

    fn assert_boots(plan: &ServicePlan) {
        let body = plan
            .files
            .iter()
            .map(|file| file.body.clone())
            .collect::<Vec<_>>()
            .join("\n");
        let starts = body.contains("WantedBy=")
            || body.contains("RunAtLoad")
            || body.contains("start= auto");
        assert!(starts, "{body}");
        let restarts = body.contains("Restart=on-failure")
            || body.contains("KeepAlive")
            || body.contains("actions= restart");
        assert!(restarts, "{body}");
    }

    #[test]
    fn each_os_starts_on_boot_and_iggy_is_first() {
        let root = std::env::temp_dir().join(format!("kova-svc-{}", std::process::id()));
        let hosts = [
            ServiceHost::SystemdUser {
                unit_dir: root.join("user"),
            },
            ServiceHost::SystemdSystem {
                unit_dir: root.join("system"),
            },
            ServiceHost::Windows {
                dir: root.join("windows"),
            },
            ServiceHost::LaunchAgent {
                dir: root.join("agent"),
            },
            ServiceHost::LaunchDaemon {
                dir: root.join("daemon"),
            },
        ];
        for host in &hosts {
            let plan = plan(host, &paths(&root, true));
            assert_boots(&plan);
            if matches!(host, ServiceHost::Windows { .. }) {
                let script = &plan.files[0].body;
                let iggy = script.find("KovaIggy").expect(script);
                let kova = script.find("sc.exe create Kova ").expect(script);
                assert!(iggy < kova, "{script}");
            } else {
                let enable = plan.enable.join("\n");
                let iggy_at = enable.find("iggy").expect(&enable);
                let kova_at = enable
                    .rfind("kova.service")
                    .or_else(|| enable.rfind("com.kova.server"))
                    .expect(&enable);
                assert!(iggy_at < kova_at, "{enable}");
            }
        }
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn removing_the_service_leaves_the_data_directory() {
        let root = std::env::temp_dir().join(format!(
            "kova-svc-rm-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        let data = root.join("data");
        fs::create_dir_all(&data).unwrap();
        fs::write(data.join("pages"), b"keep").unwrap();
        let host = ServiceHost::SystemdUser {
            unit_dir: root.join("units"),
        };
        let paths = paths(&root, false);
        apply_choice(true, &host, &paths, false).unwrap();
        assert!(root.join("units").join("kova.service").is_file());
        apply_choice(false, &host, &paths, false).unwrap();
        assert!(!root.join("units").join("kova.service").exists());
        assert_eq!(fs::read(data.join("pages")).unwrap(), b"keep");
        let _ = fs::remove_dir_all(&root);
    }
}
