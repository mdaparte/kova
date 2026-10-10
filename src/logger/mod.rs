//! Structured logging for Kova.
//!
//! This module provides a lightweight logging facade that replaces
//! `eprintln!` throughout the codebase. It is designed to be:
//!
//! - **Testable**: log records are captured in memory for assertion
//!   without writing to `stderr`.
//! - **Extensible**: external sinks can be registered to forward logs
//!   to files, syslog, or any custom consumer.

use std::collections::VecDeque;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// The monotonic timestamp of the record (milliseconds since UNIX epoch).
    pub timestamp: u128,
}

impl LogRecord {
    /// Creates a new log record with the current timestamp.
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
///
/// Sinks must be thread-safe because the global logger can be called
/// from any Tokio task.
pub trait LogSink: Send + Sync {
    /// Emits a single log record.
    fn emit(&self, record: &LogRecord);
}

/// The default sink writes logs to `stderr`.
struct StderrSink;

impl LogSink for StderrSink {
    fn emit(&self, record: &LogRecord) {
        eprintln!("[Kova] {} - {}", record.level.as_str(), record.message);
    }
}

/// The global logger facade.
pub struct Logger {
    sink: Arc<dyn LogSink>,
}

impl Logger {
    /// Creates a new logger backed by the given sink.
    pub fn new(sink: Arc<dyn LogSink>) -> Self {
        Self { sink }
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
/// The buffer is bounded: when it is full the oldest record is dropped.
/// A `Mutex` provides interior mutability so `LogSink::emit` can take
/// `&self` while still appending to the queue.
pub struct CaptureSink {
    records: Mutex<VecDeque<LogRecord>>,
    max_records: usize,
}

impl CaptureSink {
    /// Creates a new capture sink with the given capacity.
    pub fn new(max_records: usize) -> Self {
        Self {
            records: Mutex::new(VecDeque::with_capacity(max_records)),
            max_records,
        }
    }

    /// Returns the captured records, newest first.
    pub fn records(&self) -> Vec<LogRecord> {
        self.records
            .lock()
            .expect("capture sink lock poisoned")
            .iter()
            .cloned()
            .collect()
    }

    /// Clears all captured records.
    pub fn clear(&self) {
        self.records
            .lock()
            .expect("capture sink lock poisoned")
            .clear();
    }
}

impl LogSink for CaptureSink {
    fn emit(&self, record: &LogRecord) {
        let mut buffer = self.records.lock().expect("capture sink lock poisoned");
        buffer.push_front(record.clone());
        if buffer.len() > self.max_records {
            buffer.pop_back();
        }
    }
}

/// The global logger instance, initialized on first use.
///
/// `LazyLock` defers construction until the first access, which is
/// required because `Logger::new` is not a `const fn`.
pub static LOGGER: LazyLock<Logger> = LazyLock::new(|| Logger::new(Arc::new(StderrSink)));

#[cfg(test)]
mod tests {
    use super::*;

    fn test_logger() -> (Logger, Arc<CaptureSink>) {
        let sink = Arc::new(CaptureSink::new(10));
        let logger = Logger::new(sink.clone());
        (logger, sink)
    }

    #[test]
    fn test_capture_sink_records_messages() {
        let (logger, sink) = test_logger();
        logger.info("test message");
        logger.warn("warning message");

        let records = sink.records();
        assert_eq!(records.len(), 2);
        assert!(records[0].message.contains("warning"));
        assert_eq!(records[1].message, "test message");
    }

    #[test]
    fn test_capture_sink_respects_capacity() {
        let sink = Arc::new(CaptureSink::new(3));
        let logger = Logger::new(sink.clone());

        for i in 0..5 {
            logger.info(format!("message {}", i));
        }

        let records = sink.records();
        assert_eq!(records.len(), 3);
        // Newest first: message 4, 3, 2 — message 0 and 1 dropped.
        assert_eq!(records[0].message, "message 4");
        assert_eq!(records[2].message, "message 2");
    }

    #[test]
    fn test_capture_sink_clear() {
        let (logger, sink) = test_logger();
        logger.info("to be cleared");
        assert_eq!(sink.records().len(), 1);

        sink.clear();
        assert_eq!(sink.records().len(), 0);
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
        assert_eq!(Level::Debug.as_str(), "DEBUG");
        assert_eq!(Level::Info.as_str(), "INFO");
        assert_eq!(Level::Warn.as_str(), "WARN");
        assert_eq!(Level::Error.as_str(), "ERROR");
    }

    #[test]
    fn test_log_record_timestamp_is_set() {
        let record = LogRecord::new(Level::Info, "with timestamp");
        assert!(record.timestamp > 0);
    }
}
