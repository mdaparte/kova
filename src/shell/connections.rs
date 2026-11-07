//! Saved window connections. They live in a file on this machine.
//! The server never sees the file.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub last_connected: Option<u64>,
}

#[derive(Serialize, Deserialize, Default)]
struct File {
    connections: Vec<Connection>,
}

pub fn path() -> PathBuf {
    if let Some(path) = std::env::var_os("KOVA_CONNECTIONS") {
        return PathBuf::from(path);
    }
    default_path()
}

pub fn load() -> Result<Vec<Connection>, String> {
    let path = path();
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text =
        fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut file: File = serde_json::from_str(&text)
        .map_err(|err| format!("connections file is not readable: {err}"))?;
    file.connections
        .sort_by(|left, right| left.name.cmp(&right.name));
    Ok(file.connections)
}

pub fn save(connection: Connection) -> Result<Connection, String> {
    let mut connection = connection;
    connection.name = connection.name.trim().to_string();
    connection.host = connection.host.trim().to_string();
    if connection.name.is_empty() {
        return Err("name is empty".into());
    }
    if connection.host.is_empty() {
        connection.host = "127.0.0.1".into();
    }
    if connection.port == 0 {
        connection.port = 27018;
    }
    let mut connections = load()?;
    if connection.id.is_empty() {
        connection.id = new_id();
        connections.push(connection.clone());
    } else if let Some(found) = connections.iter_mut().find(|item| item.id == connection.id) {
        if connection.last_connected.is_none() {
            connection.last_connected = found.last_connected;
        }
        *found = connection.clone();
    } else {
        connections.push(connection.clone());
    }
    write(&connections)?;
    Ok(connection)
}

pub fn delete(id: &str) -> Result<(), String> {
    let mut connections = load()?;
    connections.retain(|item| item.id != id);
    write(&connections)
}

pub fn touch(id: &str) -> Result<(), String> {
    let mut connections = load()?;
    let Some(found) = connections.iter_mut().find(|item| item.id == id) else {
        return Ok(());
    };
    found.last_connected = Some(now());
    write(&connections)
}

fn write(connections: &[Connection]) -> Result<(), String> {
    let path = path();
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|err| format!("create {}: {err}", dir.display()))?;
    }
    let text = serde_json::to_string_pretty(&File {
        connections: connections.to_vec(),
    })
    .map_err(|err| err.to_string())?;
    let temp = path.with_extension("json.tmp");
    write_private(&temp, text.as_bytes())?;
    fs::rename(&temp, &path).map_err(|err| format!("save {}: {err}", path.display()))?;
    Ok(())
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|err| format!("save {}: {err}", path.display()))?;
    file.write_all(bytes)
        .map_err(|err| format!("save {}: {err}", path.display()))?;
    Ok(())
}

fn new_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("{:x}{n:x}", now())
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn default_path() -> PathBuf {
    #[cfg(windows)]
    {
        let base = std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        return base.join("kova").join("connections.json");
    }
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        return home
            .join("Library")
            .join("Application Support")
            .join("kova")
            .join("connections.json");
    }
    #[cfg(not(any(windows, target_os = "macos")))]
    {
        if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
            return PathBuf::from(xdg).join("kova").join("connections.json");
        }
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        home.join(".config").join("kova").join("connections.json")
    }
}
