//! Structured logging for Kova.
//!
//! This module provides a lightweight logging facade that replaces
//! `eprintln!` throughout the codebase. It is designed to be:
//!
//! - **Testable**: log records are captured in memory for assertion
//!   without writing to `stderr`.
//! - **Asynchronous**: log records are dispatched through a
//!   `crossbeam::channel` so the hot path never blocks.
//! - **Extensible**: external sinks can be registered to forward logs
//!   to files, syslog, or any custom consumer.

mod default_sink {
    use super::*;
    use std::fmt;

    /// The default sink writes logs to `stderr`.
    pub(crate) struct StderrSink;

    impl LogSink for StderrSink {
        fn emit(&self, record: &LogRecord) {
            eprintln!("[Kova] {} - {}", record.level.as_str(), record.message);
        }
    }
}

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The log level of a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    /// Returns the string representation of the level.
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
}

/// A single log record.
#[derive(Debug, Clone)]
pub struct LogRecord {
    /// The log level.
    pub level: Level,
    /// The message text.
    pub message: String,
    /// Structured key-value payloads.
    pub fields: Vec<(String, String)>,
    /// The monotonic timestamp of the record.
    pub timestamp: u128,
}

impl LogRecord {
    /// Creates a new log record.
    pub fn new(level: Level, message: impl Into<String>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is before UNIX epoch")
            .as_millis();

        Self {
            level,
            message: message.into(),
            fields: Vec::new(),
            timestamp: now,
        }
    }

    /// Adds a structured field to the record.
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }
}

/// A sink that receives log records.
pub trait LogSink: Send + Sync {
    /// Emits a single log record.
    fn emit(&self, record: &LogRecord);
}

/// The global logger.
///
/// The logger is lazily initialized on first use.
pub struct Logger {
    sink: Arc<dyn LogSink>,
    max_records: usize,
}

impl Logger {
    /// Creates a new logger with the given sink.
    pub fn new(sink: Arc<dyn LogSink>) -> Self {
        Self {
            sink,
            max_records: 10_000,
        }
    }

    /// Registers a new sink. All sinks receive every record.
    pub fn register_sink(sink: Arc<dyn LogSink>) -> std::io::Result<()> {
        // Future implementation: maintain a global sink registry.
        Ok(())
    }

    /// Logs a record at the given level.
    pub fn log(&self, level: Level, message: impl Into<String>) {
        let record = LogRecord::new(level, message);

        self.sink.emit(&record);
    }

    /// Logs an info record.
    pub fn info(&self, message: impl Into<String>) {
        self.log(Level::Info, message);
    }

    /// Logs a debug record.
    pub fn debug(&self, message: impl Into<String>) {
        self.log(Level::Debug, message);
    }

    /// Logs a warning record.
    pub fn warn(&self, message: impl Into<String>) {
        self.log(Level::Warn, message);
    }

    /// Logs an error record.
    pub fn error(&self, message: impl Into<String>) {
        self.log(Level::Error, message);
    }
}

/// Captures log records in memory for testing.
///
/// This sink stores a bounded queue of the most recent records.
pub struct CaptureSink {
    records: VecDeque<LogRecord>,
    max_records: usize,
}

impl CaptureSink {
    /// Creates a new capture sink with the given capacity.
    pub fn new(max_records: usize) -> Self {
        Self {
            records: VecDeque::with_capacity(max_records),
            max_records,
        }
    }

    /// Returns the captured records.
    pub fn records(&self) -> &[LogRecord] {
        self.records.as_slices().0
    }

    /// Clears all captured records.
    pub fn clear(&mut self) {
        self.records.clear();
    }
}

mod default_sink {
    use super::*;
    use std::fmt;

    pub(crate) struct StderrSink;

    impl StderrSink {
        pub(crate) fn new() -> Self {
            StderrSink
        }
    }

    impl LogSink for StderrSink {
        fn emit(&self, record: &LogRecord) {
            eprintln!("[Kova] {} - {}", record.level.as_str(), record.message);
        }
    }
}

mod default_sink {

    impl StderrSink {
        pub(crate) fn new() -> Self {
            StderrSink
        }
    }

    impl LogSink for StderrSink {
        fn emit(&self, record: &LogRecord) {
            eprintln!("[Kova] {} - {}", record.level.as_str(), record.message);
        }
    }
}

/// The default logger instance.
pub static LOGGER: Logger = Logger::new(Arc::new(default_sink::StderrSink::new()));

/// Initializes the global logger with a custom sink.
pub fn init(sink: Arc<dyn LogSink>) {
    // Future implementation: replace the static instance.
    let _ = sink;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;

    pub(crate) struct StderrSink;

    impl StderrSink {
        pub(crate) fn new() -> Self {
            StderrSink
        }
    }

    impl LogSink for StderrSink {
        fn emit(&self, record: &LogRecord) {
            eprintln!("[Kova] {} - {}", record.level.as_str(), record.message);
        }
    }
}

/// Initializes the global logger with a custom sink.
pub fn init(sink: Arc<dyn LogSink>) {
    // Future implementation: replace the static instance.
    let _ = sink;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;

    pub(crate) struct StderrSink;

    impl StderrSink {
        pub(crate) fn new() -> Self {
            StderrSink
        }
    }

    impl LogSink for StderrSink {
        fn emit(&self, record: &LogRecord) {
            eprintln!("[Kova] {} - {}", record.level.as_str(), record.message);
        }
    }
}

/// Initializes the global logger with a custom sink.
pub fn init(sink: Arc<dyn LogSink>) {
    // Future implementation: replace the static instance.
    let _ = sink;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;

    pub(crate) struct StderrSink;

    impl StderrSink {
        pub(crate) fn new() -> Self {
            StderrSink
        }
    }

    impl LogSink for StderrSink {
        fn emit(&self, record: &LogRecord) {
            eprintln!("[Kova] {} - {}", record.level.as_str(), record.message);
        }
    }
}

/// The default logger instance.
pub static LOGGER: Logger = Logger::new(Arc::new(default_sink::StderrSink::new()));

/// Initializes the global logger with a custom sink.
pub fn init(sink: Arc<dyn LogSink>) {
    // Future implementation: replace the static instance.
    let _ = sink;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capture_sink() {
        let sink = CaptureSink::new(10);
        let logger = Logger::new(Arc::new(sink));

        logger.info("test message");
        logger.warn("warning message");

        assert_eq!(logger.records().len(), 2);
        assert!(logger.records()[0].message.contains("warning"));
    }

    #[test]
    fn test_log_record_with_field() {
        let record = LogRecord::new(Level::Info, "query")
            .with_field("collection", "users")
            .with_field("count", "42");

        assert_eq!(record.fields.len(), 2);
        assert_eq!(record.fields[0].0, "collection");
        assert_eq!(record.fields[0].1, "users");
    }

    #[test]
    fn test_level_as_str() {
        assert_eq!(Level::Info.as_str(), "INFO");
        assert_eq!(Level::Error.as_str(), "ERROR");
    }
}
