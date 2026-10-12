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
use std::sync::atomic::{AtomicU8, Ordering};
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

    /// Parses a level from a case-insensitive configuration string.
    ///
    /// Returns `None` for unknown values so the caller can warn and
    /// fall back to the previous/default level.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "debug" => Some(Level::Debug),
            "info" => Some(Level::Info),
            "warn" => Some(Level::Warn),
            "error" => Some(Level::Error),
            _ => None,
        }
    }

    /// Returns the ordering rank of the level (higher = more severe).
    ///
    /// Used by the minimum-level filter: a record is emitted when its
    /// rank is greater than or equal to the configured minimum.
    fn rank(self) -> u8 {
        match self {
            Level::Debug => 0,
            Level::Info => 1,
            Level::Warn => 2,
            Level::Error => 3,
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
    /// Wall-clock time of the record, milliseconds since the UNIX epoch.
    pub timestamp: u128,
}

impl LogRecord {
    /// Creates a new log record with the current timestamp.
    pub fn new(level: Level, message: impl Into<String>) -> Self {
        Self {
            level,
            message: message.into(),
            fields: Vec::new(),
            timestamp: now_millis(),
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
        eprintln!(
            "[Kova] {} {} - {}",
            format_timestamp(record.timestamp),
            record.level.as_str(),
            record.message
        );
    }
}

/// Current wall-clock time in milliseconds since the UNIX epoch.
pub(crate) fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0)
}

/// Formats epoch milliseconds as local `YYYY-MM-DD HH:MM:SS.mmm`.
///
/// Local time is what an operator compares against the clock on the
/// machine. The millisecond field is the remainder of the epoch instant,
/// so two records in the same second stay ordered.
pub(crate) fn format_timestamp(epoch_millis: u128) -> String {
    let secs = (epoch_millis / 1000) as i64;
    let millis = (epoch_millis % 1000) as u32;
    let (year, month, day, hour, minute, second) = local_civil(secs);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{millis:03}")
}

/// Broken-down local civil time for `secs` since the UNIX epoch.
fn local_civil(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    #[cfg(unix)]
    {
        #[repr(C)]
        struct Tm {
            tm_sec: i32,
            tm_min: i32,
            tm_hour: i32,
            tm_mday: i32,
            tm_mon: i32,
            tm_year: i32,
            tm_wday: i32,
            tm_yday: i32,
            tm_isdst: i32,
            tm_gmtoff: i64,
            tm_zone: *const i8,
        }

        extern "C" {
            fn localtime_r(timep: *const i64, result: *mut Tm) -> *mut Tm;
        }

        unsafe {
            let mut tm = std::mem::zeroed();
            if !localtime_r(&secs, &mut tm).is_null() {
                return (
                    tm.tm_year + 1900,
                    (tm.tm_mon + 1) as u32,
                    tm.tm_mday as u32,
                    tm.tm_hour as u32,
                    tm.tm_min as u32,
                    tm.tm_sec as u32,
                );
            }
        }
    }

    civil_utc(secs)
}

/// Broken-down UTC civil time. Used when the local conversion is unavailable.
fn civil_utc(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400) as u32;
    let hour = tod / 3_600;
    let minute = (tod % 3_600) / 60;
    let second = tod % 60;

    // Howard Hinnant's civil_from_days, shifted to the Unix epoch.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    (year as i32, month as u32, day as u32, hour, minute, second)
}

/// The global logger facade.
pub struct Logger {
    sink: Arc<dyn LogSink>,
    /// Minimum level a record must reach to be emitted. Configured
    /// once at startup from `log_level` (defaults to `Debug`, i.e.
    /// everything is emitted).
    min_level: AtomicU8,
}

impl Logger {
    /// Creates a new logger backed by the given sink.
    pub fn new(sink: Arc<dyn LogSink>) -> Self {
        Self {
            sink,
            min_level: AtomicU8::new(Level::Debug.rank()),
        }
    }

    /// Sets the minimum level a record must reach to be emitted.
    pub fn set_min_level(&self, level: Level) {
        self.min_level.store(level.rank(), Ordering::Relaxed);
    }

    /// Logs a record at the given level.
    ///
    /// Records below the configured minimum level are dropped before
    /// touching the sink, so a quiet server never pays for formatting.
    pub fn log(&self, level: Level, message: impl Into<String>) {
        if level.rank() < self.min_level.load(Ordering::Relaxed) {
            return;
        }
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

    #[test]
    fn test_civil_utc_epoch_and_known_date() {
        assert_eq!(civil_utc(0), (1970, 1, 1, 0, 0, 0));
        // 1980-12-17 00:00:00 UTC.
        assert_eq!(civil_utc(345_859_200), (1980, 12, 17, 0, 0, 0));
    }

    #[test]
    fn test_format_timestamp_keeps_millis() {
        let rendered = format_timestamp(345_081_600_128);
        assert!(
            rendered.ends_with(".128"),
            "millis must survive the clock conversion: {rendered}"
        );
        assert_eq!(rendered.len(), "1980-12-17 00:00:00.128".len());
    }
}
