//! Runtime configuration for the Kova engine.
//!
//! Configuration is resolved in strict precedence order:
//!
//! 1. **Environment variables** (highest) — `KOVA_PORT`, `KOVA_LOG_LEVEL`,
//!    `KOVA_WARN_NON_MONGO_QUERIES`, `KOVA_DATA_DIR`, `KOVA_PAGE_SIZE`,
//!    `KOVA_BUFFER_POOL_PAGES`, `KOVA_WAL_FSYNC`,
//!    `KOVA_SECURITY_ENABLED`, `KOVA_ENCRYPTION_KEY`,
//!    `KOVA_MAX_CONNECTIONS`, `KOVA_MAX_CONNECTION_IDLE_MS`.
//! 2. **Configuration file** — `kova.toml` in the working directory, or
//!    the path given by `KOVA_CONFIG`.
//! 3. **Built-in defaults** (lowest).
//!
//! A malformed file aborts startup with a clear message (an engine that
//! ignores its own configuration file is worse than one that refuses to
//! boot). A malformed *environment* value only logs a warning and keeps
//! the previous value, so a typo never takes the server down.
//!
//! The resolved configuration is exposed through the [`CONFIG`] static,
//! following the same `LazyLock` pattern as [`crate::logger::LOGGER`],
//! so deep call sites (protocol parsing, search indexing) do not need
//! the value threaded through every constructor.

use std::sync::LazyLock;

use serde::Deserialize;

use crate::logger::Level;

/// Path of the configuration file, relative to the working directory.
pub const CONFIG_FILE: &str = "kova.toml";

/// Environment variable that overrides [`CONFIG_FILE`]'s location.
const CONFIG_PATH_ENV: &str = "KOVA_CONFIG";

/// Environment variable overrides, applied on top of the file.
const PORT_ENV: &str = "KOVA_PORT";
const LOG_LEVEL_ENV: &str = "KOVA_LOG_LEVEL";
const WARN_ENV: &str = "KOVA_WARN_NON_MONGO_QUERIES";
const DATA_DIR_ENV: &str = "KOVA_DATA_DIR";
const PAGE_SIZE_ENV: &str = "KOVA_PAGE_SIZE";
const POOL_PAGES_ENV: &str = "KOVA_BUFFER_POOL_PAGES";
const WAL_FSYNC_ENV: &str = "KOVA_WAL_FSYNC";
const SECURITY_ENABLED_ENV: &str = "KOVA_SECURITY_ENABLED";
const ENCRYPTION_KEY_ENV: &str = "KOVA_ENCRYPTION_KEY";
const MAX_CONNECTIONS_ENV: &str = "KOVA_MAX_CONNECTIONS";
const MAX_CONNECTION_IDLE_ENV: &str = "KOVA_MAX_CONNECTION_IDLE_MS";

fn default_max_connections() -> usize {
    DEFAULT_MAX_CONNECTIONS
}

fn default_idle_ms() -> u64 {
    0
}

/// Default TCP port. 27018 leaves 27017 free for a local mongod.
const DEFAULT_PORT: u16 = 27018;

/// Simultaneous clients the listener will keep. One more is accepted,
/// answered with an error, and closed.
const DEFAULT_MAX_CONNECTIONS: usize = 128;

/// The global configuration, resolved on first access.
pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    Config::load().unwrap_or_else(|err| {
        // Unrecoverable: refusing to start beats running with settings
        // the operator never chose.
        eprintln!(
            "[Kova] {} CONFIG ERROR: {}",
            crate::logger::format_timestamp(crate::logger::now_millis()),
            err
        );
        std::process::exit(1);
    })
});

/// The engine's resolved configuration.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct Config {
    /// TCP port for the MongoDB wire protocol listener.
    pub port: u16,
    /// Simultaneous clients. Past this the listener accepts the socket,
    /// replies with an error, and closes it without counting it.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Milliseconds without a command before the connection is closed.
    /// `0` leaves it open until the client or the server closes it.
    #[serde(default = "default_idle_ms")]
    pub max_connection_idle_ms: u64,
    /// Minimum log level: `"debug"`, `"info"`, `"warn"` or `"error"`.
    ///
    /// Kept as a string here so unknown values produce a friendly
    /// warning instead of a deserialization error; [`Config::log_level`]
    /// performs the validated conversion.
    pub log_level: String,
    /// Emit a `WARN` log whenever a query uses operators MongoDB does
    /// not define (Kova extensions such as `$text.$boost`, or
    /// operators the engine does not implement yet).
    pub warn_on_non_mongo_queries: bool,
    /// Full-text search behavior (Phase 4).
    pub search: SearchConfig,
    /// Disk layout, WAL, and buffer pool (Phase 6).
    pub storage: StorageConfig,
    /// Replica set. Absent or `enabled = false` keeps a single writable node.
    pub replication: ReplicationConfig,
    /// Users, roles, and the optional encryption key (Phase 13).
    pub security: SecurityConfig,
}

/// Full-text search configuration.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct SearchConfig {
    /// Token pipeline applied at index *and* query time:
    /// `"default"` (split + lowercase) or `"en_stem"` (adds English
    /// stemming for better recall). Fixed when the index is created;
    /// a query cannot override it.
    pub analyzer: String,
    /// Extra stop words removed by the analyzer (e.g. `["the", "a"]`).
    /// Empty by default: stop-word removal is language-dependent and
    /// silently dropping terms surprises users.
    pub stopwords: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connection_idle_ms: 0,
            log_level: Level::Info.as_str().to_ascii_lowercase(),
            warn_on_non_mongo_queries: true,
            search: SearchConfig::default(),
            storage: StorageConfig::default(),
            replication: ReplicationConfig::default(),
            security: SecurityConfig::default(),
        }
    }
}

/// Authentication and encryption at rest.
///
/// `enabled = false` leaves the process open: commands do not require a
/// user. The encryption key is independent. An empty key stores pages
/// compressed and unencrypted.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct SecurityConfig {
    /// Require `authenticate` before commands that are not handshakes.
    pub enabled: bool,
    /// 64 hex characters (32 bytes). Empty disables encryption.
    pub encryption_key: String,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            encryption_key: String::new(),
        }
    }
}

fn default_set_name() -> String {
    "kova".into()
}

/// Replica set membership. Election and the oplog stay inside this process.
///
/// Leave `enabled` false (the default) and Kova answers `hello` as a
/// standalone primary. Turn it on only when every `members` entry is
/// another `kova` process.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct ReplicationConfig {
    /// Join a replica set. False keeps the standalone path.
    pub enabled: bool,
    /// This process's Raft id. Must appear in [`Self::members`].
    pub id: u64,
    /// Commit the voter set and start an election. One process in the
    /// set does this; the others wait for that log. Doing it on every
    /// process with the same `members` list is also safe.
    pub bootstrap: bool,
    /// Name reported in `hello.setName`.
    #[serde(default = "default_set_name")]
    pub set_name: String,
    /// Every member, including this process. An `arbiter` receives the
    /// oplog and does not store documents or vote.
    pub members: Vec<ReplicaMember>,
}

/// One replica-set member.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ReplicaMember {
    pub id: u64,
    /// MongoDB wire address drivers should use.
    pub client: String,
    /// Raft RPC address. Not the MongoDB port.
    pub raft: String,
    /// Learner that stores the oplog and no documents.
    #[serde(default)]
    pub arbiter: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            id: 1,
            bootstrap: false,
            set_name: default_set_name(),
            members: Vec::new(),
        }
    }
}

impl ReplicationConfig {
    fn validate(&self, port: u16) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.members.is_empty() {
            return Err(format!(
                "{CONFIG_FILE}: replication.members must list the set"
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for member in &self.members {
            if !seen.insert(member.id) {
                return Err(format!(
                    "{CONFIG_FILE}: replication member id {} is duplicated",
                    member.id
                ));
            }
            if member.client.is_empty() || member.raft.is_empty() {
                return Err(format!(
                    "{CONFIG_FILE}: replication member {} needs client and raft addresses",
                    member.id
                ));
            }
        }
        let me = self
            .members
            .iter()
            .find(|member| member.id == self.id)
            .ok_or_else(|| format!("{CONFIG_FILE}: replication.id must be one of members"))?;
        if self.bootstrap && me.arbiter {
            return Err(format!(
                "{CONFIG_FILE}: an arbiter cannot bootstrap the set"
            ));
        }
        let client_port = me
            .client
            .rsplit_once(':')
            .and_then(|(_, raw)| raw.parse::<u16>().ok());
        if client_port != Some(port) {
            return Err(format!(
                "{CONFIG_FILE}: replication member {} client {} must use the listener port {}",
                me.id, me.client, port
            ));
        }
        Ok(())
    }
}

fn default_data_dir() -> String {
    "data".to_string()
}

fn default_page_size() -> u32 {
    16 * 1024
}

fn default_pool_pages() -> usize {
    4096
}

fn default_true() -> bool {
    true
}

fn default_segment_bytes() -> u64 {
    16 * 1024 * 1024
}

fn default_checkpoint_every() -> u64 {
    128
}

/// Where the engine writes the WAL, checkpoints, and page files.
///
/// One subdirectory per partition lives under [`Self::data_dir`].
/// Queries still run against the in-memory working set; these files are
/// what make that working set survive a restart.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct StorageConfig {
    /// Root directory for every partition (`data` by default).
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// On-disk page size in bytes. Default is 16 KiB.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// How many pages the buffer pool keeps resident.
    #[serde(default = "default_pool_pages")]
    pub buffer_pool_pages: usize,
    /// `fsync` each WAL append before the mutation is applied.
    #[serde(default = "default_true")]
    pub wal_fsync: bool,
    /// Roll the WAL once a segment would pass this many bytes.
    #[serde(default = "default_segment_bytes")]
    pub wal_segment_bytes: u64,
    /// Checkpoint after this many appended records. `0` disables the
    /// periodic checkpoint; shutdown still snapshots unless
    /// [`Self::checkpoint_on_shutdown`] is false.
    #[serde(default = "default_checkpoint_every")]
    pub checkpoint_every: u64,
    /// Write a checkpoint when the partition worker stops.
    #[serde(default = "default_true")]
    pub checkpoint_on_shutdown: bool,
    /// Delete `data_dir` when the router is dropped. Tests set this so
    /// they do not share the server's data directory.
    #[serde(skip)]
    pub ephemeral: bool,
    /// AES-256-GCM key for page files and WAL records. Empty means the
    /// bytes stay compressed but not encrypted. Filled from `[security]`.
    #[serde(skip)]
    pub encryption_key: Option<[u8; 32]>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            page_size: default_page_size(),
            buffer_pool_pages: default_pool_pages(),
            wal_fsync: true,
            wal_segment_bytes: default_segment_bytes(),
            checkpoint_every: default_checkpoint_every(),
            checkpoint_on_shutdown: true,
            ephemeral: false,
            encryption_key: None,
        }
    }
}

impl StorageConfig {
    /// A private directory under the system temp dir, removed with the router.
    #[cfg(test)]
    pub fn ephemeral() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("kova-run-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(&dir);
        Self {
            data_dir: dir.to_string_lossy().into_owned(),
            page_size: 4096,
            buffer_pool_pages: 8,
            wal_fsync: false,
            wal_segment_bytes: 64 * 1024,
            checkpoint_every: 10_000,
            checkpoint_on_shutdown: true,
            ephemeral: true,
            encryption_key: None,
        }
    }
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            analyzer: "default".to_string(),
            stopwords: Vec::new(),
        }
    }
}

impl Config {
    /// Loads the configuration: defaults, then file, then environment.
    pub fn load() -> Result<Self, String> {
        let mut config = Self::default();

        // 2. File (optional): `KOVA_CONFIG` path or `kova.toml` nearby.
        let path = std::env::var(CONFIG_PATH_ENV).unwrap_or_else(|_| CONFIG_FILE.to_string());
        match std::fs::read_to_string(&path) {
            Ok(raw) => {
                config = Self::from_toml(&raw).map_err(|err| format!("{}: {}", path, err))?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // No file is fine: defaults (plus env) still apply.
            }
            Err(err) => return Err(format!("failed to read {}: {}", path, err)),
        }

        // 1. Environment (highest precedence).
        config.apply_env();
        config.install_encryption_key()?;

        config.validate()?;
        Ok(config)
    }

    /// Parses a TOML document into a [`Config`].
    pub fn from_toml(raw: &str) -> Result<Self, String> {
        toml::from_str(raw).map_err(|err| format!("invalid configuration: {}", err))
    }

    /// Overrides values from environment variables.
    ///
    /// An unset variable changes nothing; an invalid value logs a
    /// warning and keeps the value from the file/defaults.
    fn apply_env(&mut self) {
        if let Some(port) = env_override::<u16>(PORT_ENV, |raw| raw.trim().parse().ok()) {
            self.port = port;
        }
        if let Some(max) = env_override(MAX_CONNECTIONS_ENV, |raw| raw.trim().parse().ok()) {
            self.max_connections = max;
        }
        if let Some(idle) = env_override(MAX_CONNECTION_IDLE_ENV, |raw| raw.trim().parse().ok()) {
            self.max_connection_idle_ms = idle;
        }
        if let Some(level) = env_override(LOG_LEVEL_ENV, Level::parse) {
            self.log_level = level.as_str().to_ascii_lowercase();
        }
        if let Some(flag) = env_override(WARN_ENV, parse_bool) {
            self.warn_on_non_mongo_queries = flag;
        }
        if let Some(dir) = env_override(DATA_DIR_ENV, |raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }) {
            self.storage.data_dir = dir;
        }
        if let Some(page_size) = env_override(PAGE_SIZE_ENV, |raw| raw.trim().parse().ok()) {
            self.storage.page_size = page_size;
        }
        if let Some(pages) = env_override(POOL_PAGES_ENV, |raw| raw.trim().parse().ok()) {
            self.storage.buffer_pool_pages = pages;
        }
        if let Some(flag) = env_override(WAL_FSYNC_ENV, parse_bool) {
            self.storage.wal_fsync = flag;
        }
        if let Some(flag) = env_override(SECURITY_ENABLED_ENV, parse_bool) {
            self.security.enabled = flag;
        }
        if let Ok(raw) = std::env::var(ENCRYPTION_KEY_ENV) {
            self.security.encryption_key = raw;
        }
    }

    /// Copies a 32-byte hex key onto [`StorageConfig::encryption_key`].
    ///
    /// A non-empty value that is not 64 hex digits fails the load. An
    /// empty value leaves pages compressed and unencrypted.
    fn install_encryption_key(&mut self) -> Result<(), String> {
        let raw = self.security.encryption_key.trim();
        if raw.is_empty() {
            self.storage.encryption_key = None;
            return Ok(());
        }
        if raw.len() != 64 || !raw.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(format!(
                "{CONFIG_FILE}: security.encryption_key must be 64 hex characters"
            ));
        }
        let mut key = [0u8; 32];
        for (index, byte) in key.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&raw[index * 2..index * 2 + 2], 16)
                .expect("hex digits checked above");
        }
        self.storage.encryption_key = Some(key);
        Ok(())
    }

    /// Returns the validated minimum log level.
    pub fn log_level(&self) -> Level {
        Level::parse(&self.log_level).unwrap_or(Level::Info)
    }

    /// Checks the resolved configuration for unusable values.
    fn validate(&self) -> Result<(), String> {
        if self.port == 0 {
            return Err(format!("{}: port must be non-zero", CONFIG_FILE));
        }
        if self.max_connections == 0 {
            return Err(format!(
                "{}: max_connections must be at least 1",
                CONFIG_FILE
            ));
        }
        if Level::parse(&self.log_level).is_none() {
            return Err(format!(
                "{}: log_level {:?} must be one of debug, info, warn, error",
                CONFIG_FILE, self.log_level
            ));
        }
        if !matches!(self.search.analyzer.as_str(), "default" | "en_stem") {
            return Err(format!(
                "{}: search.analyzer {:?} must be \"default\" or \"en_stem\"",
                CONFIG_FILE, self.search.analyzer
            ));
        }
        self.storage.validate()?;
        self.replication.validate(self.port)?;
        Ok(())
    }
}

impl StorageConfig {
    fn validate(&self) -> Result<(), String> {
        if self.data_dir.trim().is_empty() {
            return Err(format!(
                "{}: storage.data_dir must not be empty",
                CONFIG_FILE
            ));
        }
        if self.page_size < 128 || self.page_size > 16 * 1024 * 1024 {
            return Err(format!(
                "{}: storage.page_size {} must be between 128 and 16777216",
                CONFIG_FILE, self.page_size
            ));
        }
        if self.buffer_pool_pages == 0 {
            return Err(format!(
                "{}: storage.buffer_pool_pages must be at least 1",
                CONFIG_FILE
            ));
        }
        if self.wal_segment_bytes < 64 {
            return Err(format!(
                "{}: storage.wal_segment_bytes must be at least 64",
                CONFIG_FILE
            ));
        }
        Ok(())
    }
}

/// Reads `name` from the environment and maps it, warning on garbage.
fn env_override<T>(name: &str, parse: impl Fn(&str) -> Option<T>) -> Option<T> {
    match std::env::var(name) {
        Ok(raw) => match parse(&raw) {
            Some(value) => Some(value),
            None => {
                crate::logger::LOGGER.warn(format!(
                    "Ignoring invalid {}={:?}; keeping previous value",
                    name, raw
                ));
                None
            }
        },
        Err(_) => None,
    }
}

/// Parses the common truthy/falsey spellings used by env vars.
fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_defaults() {
        let config = Config::default();
        assert_eq!(config.port, 27018);
        assert_eq!(config.max_connections, 128);
        assert_eq!(config.max_connection_idle_ms, 0);
        assert_eq!(config.log_level(), Level::Info);
        assert!(config.warn_on_non_mongo_queries);
        assert_eq!(config.search, SearchConfig::default());
        assert_eq!(config.storage, StorageConfig::default());
        assert!(config.storage.wal_fsync);
        assert!(config.storage.checkpoint_on_shutdown);
    }

    #[test]
    fn test_connection_cap_comes_from_the_file() {
        let config = Config::from_toml(
            r#"
            max_connections = 4
            max_connection_idle_ms = 1000
            "#,
        )
        .expect("valid toml");
        config.validate().expect("cap is usable");
        assert_eq!(config.max_connections, 4);
        assert_eq!(config.max_connection_idle_ms, 1000);

        let closed = Config::from_toml("max_connections = 0").expect("parse ok");
        let err = closed.validate().expect_err("zero cap");
        assert!(err.contains("max_connections"), "{err}");
    }

    #[test]
    fn test_from_toml_overrides_defaults() {
        let config = Config::from_toml(
            r#"
            port = 27019
            log_level = "debug"
            warn_on_non_mongo_queries = false

            [search]
            analyzer = "en_stem"
            stopwords = ["the", "a"]
            "#,
        )
        .expect("valid toml");

        assert_eq!(config.port, 27019);
        assert_eq!(config.log_level(), Level::Debug);
        assert!(!config.warn_on_non_mongo_queries);
        assert_eq!(config.search.analyzer, "en_stem");
        assert_eq!(config.search.stopwords, vec!["the", "a"]);
        assert_eq!(config.storage.page_size, 16 * 1024);
        assert!(config.storage.wal_fsync);
    }

    #[test]
    fn test_storage_section_overrides_only_the_keys_it_sets() {
        let config = Config::from_toml(
            r#"
            [storage]
            page_size = 8192
            wal_fsync = false
            "#,
        )
        .expect("valid toml");
        config.validate().expect("storage section is valid");
        assert_eq!(config.storage.page_size, 8192);
        assert!(!config.storage.wal_fsync);
        assert_eq!(config.storage.data_dir, "data");
        assert_eq!(config.storage.buffer_pool_pages, 4096);
        assert!(config.storage.checkpoint_on_shutdown);
    }

    #[test]
    fn test_storage_page_size_is_rejected_when_too_small() {
        let config = Config::from_toml("[storage]\npage_size = 16").expect("parse ok");
        let err = config.validate().expect_err("page size");
        assert!(err.contains("page_size"), "{}", err);
    }

    #[test]
    fn test_from_toml_partial_file_keeps_other_defaults() {
        let config = Config::from_toml("port = 30000").expect("valid toml");
        assert_eq!(config.port, 30000);
        assert_eq!(config.max_connections, 128);
        assert_eq!(config.max_connection_idle_ms, 0);
        assert_eq!(config.log_level(), Level::Info);
        assert_eq!(config.search.analyzer, "default");
    }

    #[test]
    fn test_from_toml_rejects_invalid_values() {
        assert!(Config::from_toml("log_level = 42").is_err());
        assert!(Config::from_toml("port = \"yes\"").is_err());
        assert!(Config::from_toml("this is not toml =").is_err());
    }

    #[test]
    fn test_validate_rejects_unknown_analyzer() {
        let mut config = Config::default();
        config.search.analyzer = "solr".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_accepts_known_analyzers() {
        for analyzer in ["default", "en_stem"] {
            let mut config = Config::default();
            config.search.analyzer = analyzer.into();
            assert!(config.validate().is_ok(), "analyzer {}", analyzer);
        }
    }

    #[test]
    fn test_encryption_key_must_be_64_hex_characters() {
        let mut config =
            Config::from_toml("[security]\nencryption_key = \"abcd\"").expect("parse ok");
        let err = config.install_encryption_key().expect_err("short key");
        assert!(err.contains("64 hex"), "{err}");

        config.security.encryption_key = "ab".repeat(32);
        config.install_encryption_key().expect("32-byte key");
        assert_eq!(config.storage.encryption_key.unwrap()[0], 0xab);
        assert!(!config.security.enabled);
    }

    #[test]
    fn test_parse_bool_spellings() {
        for raw in ["true", "TRUE", "1", "yes", "on"] {
            assert!(parse_bool(raw) == Some(true), "{}", raw);
        }
        for raw in ["false", "0", "no", "off"] {
            assert!(parse_bool(raw) == Some(false), "{}", raw);
        }
        assert_eq!(parse_bool("maybe"), None);
    }

    #[test]
    fn test_level_parse_is_case_insensitive() {
        assert_eq!(Level::parse("DEBUG"), Some(Level::Debug));
        assert_eq!(Level::parse(" Warn "), Some(Level::Warn));
        assert_eq!(Level::parse("loud"), None);
    }

    #[test]
    fn test_replication_section_is_optional_and_parsed() {
        let standalone = Config::from_toml("port = 27018").expect("parse");
        assert!(!standalone.replication.enabled);
        standalone.validate().expect("standalone");

        let config = Config::from_toml(
            r#"
            port = 27018
            [replication]
            enabled = true
            id = 1
            bootstrap = true
            members = [
              { id = 1, client = "127.0.0.1:27018", raft = "127.0.0.1:27118" },
              { id = 2, client = "127.0.0.1:27019", raft = "127.0.0.1:27119", arbiter = true },
            ]
            "#,
        )
        .expect("parse");
        assert!(config.replication.enabled);
        assert!(config.replication.members[1].arbiter);
        config.validate().expect("set");
    }

    #[test]
    fn test_from_toml_rejects_unknown_analyzer() {
        let config = Config::from_toml("[search]\nanalyzer = \"lucene9\"").expect("parse ok");
        let err = config.validate().expect_err("validate fails");
        assert!(err.contains("analyzer"), "unexpected error: {}", err);
    }
}
