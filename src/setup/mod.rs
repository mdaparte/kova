//! Configuration walkthrough.
//!
//! `kova-setup` writes `kova.toml` and opens again later with the current
//! values filled in. The first run suggests a free port, an absolute data
//! directory, and security closed. Leaving security open requires the
//! confirmation `OPEN`. The engine does not start `iggy-server`.

mod banner;
mod cli;
pub mod service;

#[cfg(windows)]
mod winsvc;

#[cfg(windows)]
pub use winsvc::run_kova as run_kova_service;

pub use cli::run;

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use toml_edit::{DocumentMut, Item};

use crate::config::Config;

/// Answers written into `kova.toml`. The root password is not one of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answers {
    pub port: u16,
    pub data_dir: String,
    pub run_iggy: bool,
    pub iggy_user: String,
    /// `None` keeps the password already in the file.
    pub iggy_password: Option<String>,
    /// `true` is closed: commands require a user.
    pub security_enabled: bool,
    pub log_target: String,
    pub log_rotate: String,
    pub log_max_bytes: u64,
    pub log_keep: u64,
}

/// Values the walkthrough shows. Enter accepts them.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Suggestion {
    pub first_run: bool,
    pub port: u16,
    pub data_dir: String,
    pub run_iggy: bool,
    pub iggy_user: String,
    pub iggy_password: String,
    pub keep_iggy_password: bool,
    /// Closed. The file value on a later run.
    pub security_enabled: bool,
    pub root_user: String,
    pub root_password: String,
    pub log_target: String,
    pub log_rotate: String,
    pub log_max_bytes: u64,
    pub log_keep: u64,
    /// Whether a service unit is already installed. The walkthrough fills this in.
    pub service_installed: bool,
}

pub enum RootUser {
    Created,
    AlreadyThere,
}

/// `true` only for the exact confirmation.
pub fn security_open_confirmed(typed: &str) -> bool {
    typed.trim() == "OPEN"
}

/// `leave_open` is the checkbox. The returned bool is `[security] enabled`.
///
/// A later run that is already open can stay open without typing `OPEN` again.
/// Switching from closed to open still requires that confirmation.
pub fn resolve_security(
    suggested_closed: bool,
    leave_open: bool,
    confirmation: &str,
) -> Result<bool, String> {
    if !leave_open {
        return Ok(true);
    }
    if !suggested_closed || security_open_confirmed(confirmation) {
        return Ok(false);
    }
    Err("security stays closed unless the confirmation is OPEN".into())
}

pub fn generate_password() -> String {
    random_string(
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
        20,
    )
}

pub fn generate_user() -> String {
    format!(
        "kova-{}",
        random_string(b"abcdefghijklmnopqrstuvwxyz0123456789", 6)
    )
}

fn random_string(alphabet: &[u8], len: usize) -> String {
    let limit = alphabet.len() * (256 / alphabet.len());
    let mut out = String::with_capacity(len);
    let mut byte = [0u8; 1];
    while out.len() < len {
        getrandom::getrandom(&mut byte).expect("os entropy");
        let value = byte[0] as usize;
        if value < limit {
            out.push(alphabet[value % alphabet.len()] as char);
        }
    }
    out
}

pub fn port_free(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// `start` when it is free, otherwise the next free port above it.
pub fn suggest_port(start: u16) -> u16 {
    let mut port = start.max(1);
    for _ in 0..128 {
        if port_free(port) {
            return port;
        }
        if port == u16::MAX {
            break;
        }
        port += 1;
    }
    start
}

pub fn suggest_data_dir() -> PathBuf {
    suggest_data_dir_in(
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from),
        std::env::var_os("XDG_DATA_HOME"),
        std::env::var_os("LOCALAPPDATA"),
    )
}

pub fn suggest_data_dir_in(
    home: Option<PathBuf>,
    xdg: Option<std::ffi::OsString>,
    local: Option<std::ffi::OsString>,
) -> PathBuf {
    #[cfg(target_os = "windows")]
    if let Some(dir) = windows_data_dir(home.as_ref(), local.as_ref()) {
        return dir;
    }
    #[cfg(target_os = "macos")]
    if let Some(home) = home.as_ref() {
        return home
            .join("Library")
            .join("Application Support")
            .join("Kova");
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    if let Some(dir) = unix_data_dir(home.as_ref(), xdg.as_ref()) {
        return dir;
    }
    let _ = (home, xdg, local);
    PathBuf::from("data")
}

#[cfg(target_os = "windows")]
fn windows_data_dir(home: Option<&PathBuf>, local: Option<&std::ffi::OsString>) -> Option<PathBuf> {
    if let Some(local) = local.filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(local).join("Kova"));
    }
    home.map(|home| home.join("AppData").join("Local").join("Kova"))
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
fn unix_data_dir(home: Option<&PathBuf>, xdg: Option<&std::ffi::OsString>) -> Option<PathBuf> {
    if let Some(xdg) = xdg.filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(xdg).join("kova"));
    }
    home.map(|home| home.join(".local").join("share").join("kova"))
}

pub fn suggest(raw: &str) -> Result<Suggestion, String> {
    if raw.trim().is_empty() {
        return Ok(Suggestion {
            first_run: true,
            port: suggest_port(27018),
            data_dir: suggest_data_dir().display().to_string(),
            run_iggy: false,
            iggy_user: generate_user(),
            iggy_password: generate_password(),
            keep_iggy_password: false,
            security_enabled: true,
            root_user: generate_user(),
            root_password: generate_password(),
            log_target: "stderr".into(),
            log_rotate: "none".into(),
            log_max_bytes: 10 * 1024 * 1024,
            log_keep: 7,
            service_installed: false,
        });
    }
    let doc = raw
        .parse::<DocumentMut>()
        .map_err(|err| format!("invalid configuration: {err}"))?;
    let iggy_password = nested_str(&doc, &["feed", "password"]).unwrap_or_default();
    Ok(Suggestion {
        first_run: false,
        port: top_int(&doc, "port").unwrap_or(27018) as u16,
        data_dir: nested_str(&doc, &["storage", "data_dir"]).unwrap_or_else(|| "data".into()),
        run_iggy: nested_bool(&doc, &["feed", "server", "run"]).unwrap_or(false),
        iggy_user: {
            let user = nested_str(&doc, &["feed", "username"]).unwrap_or_default();
            if user.is_empty() {
                generate_user()
            } else {
                user
            }
        },
        iggy_password: if iggy_password.is_empty() {
            generate_password()
        } else {
            String::new()
        },
        keep_iggy_password: !iggy_password.is_empty(),
        security_enabled: nested_bool(&doc, &["security", "enabled"]).unwrap_or(false),
        root_user: generate_user(),
        root_password: generate_password(),
        log_target: nested_str(&doc, &["log", "target"]).unwrap_or_else(|| "stderr".into()),
        log_rotate: nested_str(&doc, &["log", "rotate"]).unwrap_or_else(|| "none".into()),
        log_max_bytes: nested_int(&doc, &["log", "max_bytes"]).unwrap_or(10 * 1024 * 1024) as u64,
        log_keep: nested_int(&doc, &["log", "keep"]).unwrap_or(7) as u64,
        service_installed: false,
    })
}

/// Merges `answers` into `raw` and creates the data directory.
///
/// Keys the questions do not cover stay as they are. The root password
/// is not written.
pub fn apply(raw: &str, answers: &Answers) -> Result<String, String> {
    let mut doc = if raw.trim().is_empty() {
        DocumentMut::new()
    } else {
        raw.parse::<DocumentMut>()
            .map_err(|err| format!("invalid configuration: {err}"))?
    };
    doc["port"] = toml_edit::value(i64::from(answers.port));
    ensure_table(&mut doc, "storage")?;
    doc["storage"]["data_dir"] = toml_edit::value(answers.data_dir.as_str());
    ensure_table(&mut doc, "security")?;
    doc["security"]["enabled"] = toml_edit::value(answers.security_enabled);
    ensure_table(&mut doc, "log")?;
    let target = if answers.log_target.trim().is_empty() {
        "stderr"
    } else {
        answers.log_target.trim()
    };
    let rotate = if target == "stderr" {
        "none"
    } else {
        answers.log_rotate.trim()
    };
    doc["log"]["target"] = toml_edit::value(target);
    doc["log"]["rotate"] = toml_edit::value(rotate);
    doc["log"]["max_bytes"] = toml_edit::value(answers.log_max_bytes as i64);
    doc["log"]["keep"] = toml_edit::value(answers.log_keep as i64);

    ensure_table(&mut doc, "feed")?;
    ensure_table_at(&mut doc, "feed", "server")?;
    doc["feed"]["server"]["run"] = toml_edit::value(answers.run_iggy);
    if answers.run_iggy {
        doc["feed"]["enabled"] = toml_edit::value(true);
        if nested_str(&doc, &["feed", "address"])
            .unwrap_or_default()
            .is_empty()
        {
            doc["feed"]["address"] = toml_edit::value("127.0.0.1:8090");
        }
        if nested_str(&doc, &["feed", "stream"])
            .unwrap_or_default()
            .is_empty()
        {
            doc["feed"]["stream"] = toml_edit::value("kova");
        }
        if nested_str(&doc, &["feed", "topic"])
            .unwrap_or_default()
            .is_empty()
        {
            doc["feed"]["topic"] = toml_edit::value("changes");
        }
        if nested_str(&doc, &["feed", "server", "path"])
            .unwrap_or_default()
            .is_empty()
        {
            doc["feed"]["server"]["path"] = toml_edit::value("iggy-data");
        }
        if answers.iggy_user.trim().is_empty() {
            return Err("the iggy user needs a name".into());
        }
        doc["feed"]["username"] = toml_edit::value(answers.iggy_user.trim());
        match &answers.iggy_password {
            Some(password) => {
                if password.is_empty() {
                    return Err("the iggy password is empty".into());
                }
                doc["feed"]["password"] = toml_edit::value(password.as_str());
            }
            None => {
                if nested_str(&doc, &["feed", "password"])
                    .unwrap_or_default()
                    .is_empty()
                {
                    return Err("the iggy password is missing".into());
                }
            }
        }
    }

    let text = doc.to_string();
    let config = Config::from_toml(&text)?;
    config.validate()?;
    if !answers.data_dir.is_empty() {
        fs::create_dir_all(&answers.data_dir)
            .map_err(|err| format!("data directory {}: {err}", answers.data_dir))?;
    }
    Ok(text)
}

fn ensure_table(doc: &mut DocumentMut, key: &str) -> Result<(), String> {
    match doc.get(key) {
        Some(item) if item.is_table() => Ok(()),
        Some(_) => Err(format!("{key} must be a table")),
        None => {
            doc[key] = toml_edit::table();
            Ok(())
        }
    }
}

fn ensure_table_at(doc: &mut DocumentMut, parent: &str, key: &str) -> Result<(), String> {
    ensure_table(doc, parent)?;
    match doc[parent].get(key) {
        Some(item) if item.is_table() => Ok(()),
        Some(_) => Err(format!("{parent}.{key} must be a table")),
        None => {
            doc[parent][key] = toml_edit::table();
            Ok(())
        }
    }
}

fn top_int(doc: &DocumentMut, key: &str) -> Option<i64> {
    doc.get(key).and_then(Item::as_integer)
}

fn nested_str(doc: &DocumentMut, path: &[&str]) -> Option<String> {
    let item = walk(doc.as_item(), path)?;
    item.as_str().map(|value| value.to_string())
}

fn nested_bool(doc: &DocumentMut, path: &[&str]) -> Option<bool> {
    let item = walk(doc.as_item(), path)?;
    item.as_bool()
}

fn nested_int(doc: &DocumentMut, path: &[&str]) -> Option<i64> {
    let item = walk(doc.as_item(), path)?;
    item.as_integer()
}

fn walk<'a>(item: &'a Item, path: &[&str]) -> Option<&'a Item> {
    let mut current = item;
    for key in path {
        current = current.get(key)?;
    }
    Some(current)
}

pub fn create_root_user(addr: SocketAddr, user: &str, password: &str) -> Result<RootUser, String> {
    if user.is_empty() || password.is_empty() {
        return Err("the root user needs a name and a password".into());
    }
    let command = bson::doc! {
        "createUser": user,
        "pwd": password,
        "roles": ["root"],
        "$db": "admin",
    };
    let frame = crate::protocol::encode_reply(1, 0, &command)?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|err| format!("connect {addr}: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| err.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| err.to_string())?;
    stream
        .write_all(&frame)
        .map_err(|err| format!("write createUser: {err}"))?;
    let mut header = [0u8; 16];
    stream
        .read_exact(&mut header)
        .map_err(|err| format!("read createUser reply: {err}"))?;
    let length = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes")) as usize;
    if !(16..=crate::protocol::MAX_MESSAGE_SIZE).contains(&length) {
        return Err(format!("createUser reply length {length}"));
    }
    let mut body = vec![0u8; length - 16];
    stream
        .read_exact(&mut body)
        .map_err(|err| format!("read createUser body: {err}"))?;
    let reply = crate::protocol::decode_op_msg(&body)?;
    if command_ok(&reply.command) {
        return Ok(RootUser::Created);
    }
    let message = reply
        .command
        .get_str("errmsg")
        .unwrap_or("createUser failed")
        .to_string();
    if message.contains("already exists") || message.contains("authentication required") {
        return Ok(RootUser::AlreadyThere);
    }
    Err(message)
}

fn command_ok(doc: &bson::Document) -> bool {
    match doc.get("ok") {
        Some(bson::Bson::Double(value)) => *value == 1.0,
        Some(bson::Bson::Int32(value)) => *value == 1,
        Some(bson::Bson::Int64(value)) => *value == 1,
        _ => false,
    }
}

pub fn find_up(start: &Path, relative: &str) -> Option<PathBuf> {
    let mut dir = if start.is_file() {
        start.parent()?.to_path_buf()
    } else {
        start.to_path_buf()
    };
    for _ in 0..8 {
        let candidate = dir.join(relative);
        if candidate.is_file() {
            return Some(candidate);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

pub fn graphical_session() -> bool {
    #[cfg(windows)]
    {
        return true;
    }
    #[cfg(target_os = "macos")]
    {
        return true;
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        std::env::var_os("DISPLAY").is_some() || std::env::var_os("WAYLAND_DISPLAY").is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(port: u16, data: &str, security: bool) -> Answers {
        Answers {
            port,
            data_dir: data.to_string(),
            run_iggy: false,
            iggy_user: String::new(),
            iggy_password: None,
            security_enabled: security,
            log_target: "stderr".into(),
            log_rotate: "none".into(),
            log_max_bytes: 10 * 1024 * 1024,
            log_keep: 7,
        }
    }

    #[test]
    fn password_is_twenty_characters_and_not_the_sample() {
        let password = generate_password();
        assert_eq!(password.len(), 20);
        assert_ne!(password, "iggy");
        assert!(password.chars().all(|ch| ch.is_ascii_alphanumeric()));
        let user = generate_user();
        assert!(user.starts_with("kova-"));
        assert_ne!(user, "iggy");
        assert_ne!(user, "root");
    }

    #[test]
    fn open_security_requires_the_word_open() {
        assert!(!security_open_confirmed("open"));
        assert!(!security_open_confirmed("yes"));
        assert!(!security_open_confirmed(""));
        assert!(security_open_confirmed("OPEN"));
        assert!(resolve_security(true, false, "").unwrap());
        assert!(resolve_security(true, true, "open").is_err());
        assert!(!resolve_security(true, true, "OPEN").unwrap());
        assert!(!resolve_security(false, true, "").unwrap());
    }

    #[test]
    fn suggested_port_is_free_and_skips_a_taken_port() {
        let port = suggest_port(27018);
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("suggested port is free");
        drop(listener);
        let held = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let taken = held.local_addr().unwrap().port();
        let next = suggest_port(taken);
        assert_ne!(next, taken);
        assert!(port_free(next));
    }

    #[test]
    fn suggested_data_directory_is_absolute() {
        let dir = suggest_data_dir_in(Some(PathBuf::from("/home/ada")), None, None);
        assert!(dir.is_absolute());
        assert_eq!(dir, PathBuf::from("/home/ada/.local/share/kova"));
    }

    #[test]
    fn accept_every_suggestion_closes_security_and_a_later_run_can_change_the_port() {
        let root = std::env::temp_dir().join(format!(
            "kova-setup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        let data = root.join("data");
        let port = suggest_port(27018);
        let password = generate_password();
        assert_ne!(password, "iggy");
        let answers = sample(port, &data.display().to_string(), true);
        let text = apply("", &answers).unwrap();
        assert!(data.is_dir());
        assert!(!text.contains(&password));
        let config = Config::from_toml(&text).unwrap();
        assert_eq!(config.port, port);
        assert!(config.security.enabled);
        assert!(!config.feed.server.run);
        assert_ne!(config.feed.password, "iggy");
        assert!(port_free(port));

        let mut later = answers.clone();
        later.port = suggest_port(port.saturating_add(1));
        let text = apply(&text, &later).unwrap();
        let config = Config::from_toml(&text).unwrap();
        config.validate().unwrap();
        assert_eq!(config.port, later.port);
        assert_eq!(config.storage.data_dir, data.display().to_string());
        assert!(data.is_dir());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_later_run_keeps_page_size_replication_and_the_iggy_password() {
        let root = std::env::temp_dir().join(format!("kova-setup-keep-{}", std::process::id()));
        let data = root.join("db");
        let raw = r#"
port = 27018

[storage]
data_dir = "data"
page_size = 8192

[replication]
enabled = false
id = 7

[feed]
enabled = true
username = "listener"
password = "kept-secret"
address = "127.0.0.1:8090"

[feed.server]
run = true
path = "iggy-data"
"#;
        let mut answers = sample(27019, &data.display().to_string(), true);
        answers.run_iggy = true;
        answers.iggy_user = "listener".into();
        answers.iggy_password = None;
        let text = apply(raw, &answers).unwrap();
        let config = Config::from_toml(&text).unwrap();
        assert_eq!(config.storage.page_size, 8192);
        assert_eq!(config.replication.id, 7);
        assert_eq!(config.feed.password, "kept-secret");
        assert!(config.feed.server.run);
        assert!(!text.contains("abcdefghijklmnopqrst"));
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn create_root_user_on_an_empty_catalog() {
        let root = std::env::temp_dir().join(format!(
            "kova-setup-user-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&root).unwrap();
        let storage = crate::config::StorageConfig {
            data_dir: root.display().to_string(),
            page_size: 4096,
            buffer_pool_pages: 8,
            wal_fsync: false,
            wal_segment_bytes: 64 * 1024,
            checkpoint_every: 10_000,
            ephemeral: false,
            ..crate::config::StorageConfig::default()
        };
        let router = crate::storage::PartitionRouter::with_storage(1, storage);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
        let serve_router = router.clone();
        let server = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let Ok((mut socket, _)) = accepted else { break };
                        let router = serve_router.clone();
                        tokio::spawn(async move {
                            let _ = crate::network::handle_client(&mut socket, &router, None).await;
                        });
                    }
                }
            }
        });
        let password = generate_password();
        let created =
            tokio::task::spawn_blocking(move || create_root_user(addr, "kova-root", &password))
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(created, RootUser::Created));
        let again = tokio::task::spawn_blocking(move || {
            create_root_user(addr, "kova-root", "abcdefghijklmnopqrst")
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(again, RootUser::AlreadyThere));
        drop(stop);
        let _ = server.await;
        drop(router);
        let _ = fs::remove_dir_all(&root);
    }
}
