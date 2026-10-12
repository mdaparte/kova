//! Runtime configuration for the Kova engine.
//!
//! Configuration is resolved in strict precedence order:
//!
//! 1. **Environment variables** (highest) — `KOVA_PORT`, `KOVA_LOG_LEVEL`,
//!    `KOVA_WARN_NON_MONGO_QUERIES`.
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

/// Default TCP port (the standard MongoDB port).
const DEFAULT_PORT: u16 = 27018;

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
    /// Full-text search behavior (Phase 3.5).
    pub search: SearchConfig,
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
            log_level: Level::Info.as_str().to_ascii_lowercase(),
            warn_on_non_mongo_queries: true,
            search: SearchConfig::default(),
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
        if let Some(level) = env_override(LOG_LEVEL_ENV, Level::parse) {
            self.log_level = level.as_str().to_ascii_lowercase();
        }
        if let Some(flag) = env_override(WARN_ENV, parse_bool) {
            self.warn_on_non_mongo_queries = flag;
        }
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
        assert_eq!(config.log_level(), Level::Info);
        assert!(config.warn_on_non_mongo_queries);
        assert_eq!(config.search, SearchConfig::default());
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
    }

    #[test]
    fn test_from_toml_partial_file_keeps_other_defaults() {
        let config = Config::from_toml("port = 30000").expect("valid toml");
        assert_eq!(config.port, 30000);
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
    fn test_from_toml_rejects_unknown_analyzer() {
        let config = Config::from_toml("[search]\nanalyzer = \"lucene9\"").expect("parse ok");
        let err = config.validate().expect_err("validate fails");
        assert!(err.contains("analyzer"), "unexpected error: {}", err);
    }
}
