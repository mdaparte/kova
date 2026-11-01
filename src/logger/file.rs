//! File sink with rotation by date, size, or both.
//!
//! `target = "stderr"` keeps the process on the standard error stream.
//! A path appends there. `keep` is how many files stay, including the
//! one being written. An older file past that count is removed.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::{format_timestamp, local_civil, LogRecord, LogSink, LOGGER};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Rotate {
    None,
    Date,
    Size,
    DateAndSize,
}

impl Rotate {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "none" => Some(Self::None),
            "date" => Some(Self::Date),
            "size" => Some(Self::Size),
            "date-and-size" => Some(Self::DateAndSize),
            _ => None,
        }
    }

    fn by_date(self) -> bool {
        matches!(self, Self::Date | Self::DateAndSize)
    }

    fn by_size(self) -> bool {
        matches!(self, Self::Size | Self::DateAndSize)
    }
}

/// Opens the sink described by `[log]`.
///
/// `target` is `stderr` or a file path. Rotation other than `none`
/// requires a path. `keep` counts the active file.
pub(crate) fn install_log(
    target: &str,
    rotate: &str,
    max_bytes: u64,
    keep: u64,
) -> Result<(), String> {
    let sink = open_log(target, rotate, max_bytes, keep)?;
    LOGGER.set_sink(sink);
    Ok(())
}

fn open_log(
    target: &str,
    rotate: &str,
    max_bytes: u64,
    keep: u64,
) -> Result<Arc<dyn LogSink>, String> {
    let target = target.trim();
    if target.is_empty() || target == "stderr" {
        if rotate != "none" {
            return Err("log.rotate requires a file path in log.target".into());
        }
        return Ok(Arc::new(super::StderrSink));
    }
    let mode = Rotate::parse(rotate).ok_or_else(|| {
        format!("log.rotate {rotate:?} must be none, date, size, or date-and-size")
    })?;
    if keep == 0 {
        return Err("log.keep must be at least 1".into());
    }
    if mode.by_size() && max_bytes == 0 {
        return Err("log.max_bytes must be at least 1 when rotating by size".into());
    }
    let path = PathBuf::from(target);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| format!("log directory: {err}"))?;
        }
    }
    Ok(Arc::new(FileLog {
        base: path,
        mode,
        max_bytes,
        keep: keep as usize,
        state: Mutex::new(None),
        clock: Arc::new(local_date),
    }))
}

fn local_date() -> (i32, u32, u32) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0);
    let (year, month, day, _, _, _) = local_civil(secs);
    (year, month, day)
}

struct OpenFile {
    file: File,
    len: u64,
    day: String,
    path: PathBuf,
}

struct FileLog {
    base: PathBuf,
    mode: Rotate,
    max_bytes: u64,
    keep: usize,
    state: Mutex<Option<OpenFile>>,
    clock: Arc<dyn Fn() -> (i32, u32, u32) + Send + Sync>,
}

impl FileLog {
    fn write_record(&self, record: &LogRecord) -> io::Result<()> {
        let line = format!(
            "[Kova] {} {} - {}\n",
            format_timestamp(record.timestamp),
            record.level.as_str(),
            record.message
        );
        self.write_line(line.as_bytes())
    }

    fn write_line(&self, line: &[u8]) -> io::Result<()> {
        let mut guard = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let day = date_string((self.clock)());
        if guard.is_none() {
            let path = self.active_path(&day);
            *guard = Some(self.open_append(&path, &day)?);
        }
        let open = guard.as_mut().expect("opened above");
        if self.mode.by_date() && open.day != day {
            let path = self.active_path(&day);
            *open = self.open_append(&path, &day)?;
        }
        if self.mode.by_size() && open.len > 0 && open.len + line.len() as u64 > self.max_bytes {
            let path = open.path.clone();
            let day = open.day.clone();
            *open = self.roll(&path, &day)?;
        }
        open.file.write_all(line)?;
        open.file.flush()?;
        open.len += line.len() as u64;
        let active = open.path.clone();
        drop(guard);
        self.prune(&active)?;
        Ok(())
    }

    fn active_path(&self, day: &str) -> PathBuf {
        if self.mode.by_date() {
            dated_path(&self.base, day)
        } else {
            self.base.clone()
        }
    }

    fn open_append(&self, path: &Path, day: &str) -> io::Result<OpenFile> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let len = file.metadata()?.len();
        Ok(OpenFile {
            file,
            len,
            day: day.to_string(),
            path: path.to_path_buf(),
        })
    }

    fn roll(&self, active: &Path, day: &str) -> io::Result<OpenFile> {
        if self.keep <= 1 {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(active)?;
            return Ok(OpenFile {
                file,
                len: 0,
                day: day.to_string(),
                path: active.to_path_buf(),
            });
        }
        rotate_size(active, self.keep)?;
        self.open_append(active, day)
    }

    fn prune(&self, active: &Path) -> io::Result<()> {
        if self.mode == Rotate::None || self.keep == 0 {
            return Ok(());
        }
        let mut family = family_files(&self.base, self.mode)?;
        family.sort_by(|left, right| left.1 .0.cmp(&right.1 .0).then(right.1 .1.cmp(&left.1 .1)));
        let overflow = family.len().saturating_sub(self.keep);
        let mut removed = 0;
        for (path, _) in family {
            if removed >= overflow {
                break;
            }
            if path == active {
                continue;
            }
            let _ = fs::remove_file(&path);
            removed += 1;
        }
        Ok(())
    }
}

impl LogSink for FileLog {
    fn emit(&self, record: &LogRecord) {
        if let Err(err) = self.write_record(record) {
            eprintln!("[Kova] log file write failed: {err}");
        }
    }
}

fn date_string(day: (i32, u32, u32)) -> String {
    format!("{:04}-{:02}-{:02}", day.0, day.1, day.2)
}

fn dated_path(base: &Path, day: &str) -> PathBuf {
    let name = base
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "kova.log".to_string());
    let (stem, ext) = match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => (stem.to_string(), format!(".{ext}")),
        _ => (name, String::new()),
    };
    let parent = base.parent().unwrap_or_else(|| Path::new(""));
    parent.join(format!("{stem}-{day}{ext}"))
}

/// `(path, sort key)`. The sort key is `(date or empty, index)`.
/// Index 0 is the active file. A higher index is an older size roll.
fn family_files(base: &Path, mode: Rotate) -> io::Result<Vec<(PathBuf, (String, usize))>> {
    let dir = base.parent().unwrap_or_else(|| Path::new("."));
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let mut found = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(key) = family_key(base, &path, mode) {
            found.push((path, key));
        }
    }
    Ok(found)
}

fn family_key(base: &Path, path: &Path, mode: Rotate) -> Option<(String, usize)> {
    let name = path.file_name()?.to_str()?;
    if mode.by_date() {
        let dated = dated_path(base, "0000-00-00");
        let pattern = dated.file_name()?.to_str()?;
        let (prefix, suffix) = pattern.split_once("0000-00-00")?;
        if !name.starts_with(prefix) || name.len() < prefix.len() + 10 {
            return None;
        }
        let rest = &name[prefix.len()..];
        let day = &rest[..10];
        if !is_day(day) {
            return None;
        }
        let after = &rest[10..];
        if after == suffix {
            return Some((day.to_string(), 0));
        }
        let dotted = after.strip_prefix(suffix)?.strip_prefix('.')?;
        let index = dotted.parse::<usize>().ok()?;
        return Some((day.to_string(), index));
    }
    let base_name = base.file_name()?.to_str()?;
    if name == base_name {
        return Some((String::new(), 0));
    }
    let rest = name.strip_prefix(base_name)?.strip_prefix('.')?;
    let index = rest.parse::<usize>().ok()?;
    Some((String::new(), index))
}

fn is_day(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

fn rotate_size(active: &Path, keep: usize) -> io::Result<()> {
    let name = active
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let dir = active.parent().unwrap_or_else(|| Path::new("."));
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let fname = entry.file_name().to_string_lossy().into_owned();
            if let Some(rest) = fname.strip_prefix(&format!("{name}.")) {
                if rest
                    .parse::<usize>()
                    .ok()
                    .is_some_and(|index| index >= keep - 1)
                {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }
    for index in (1..keep.saturating_sub(1)).rev() {
        let from = dir.join(format!("{name}.{index}"));
        let to = dir.join(format!("{name}.{}", index + 1));
        if from.exists() {
            fs::rename(from, to)?;
        }
    }
    fs::rename(active, dir.join(format!("{name}.1")))?;
    Ok(())
}

// StderrSink is private in the parent. Re-export a constructor path by
// making the parent sink visible to this child. The parent struct is
// private, which this child can name.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::logger::{Level, LogRecord};

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kova-log-{}-{}-{}",
            std::process::id(),
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("temp log dir");
        dir
    }

    fn sink_on(
        path: PathBuf,
        mode: Rotate,
        max_bytes: u64,
        keep: usize,
        day: Arc<Mutex<(i32, u32, u32)>>,
    ) -> FileLog {
        let clock_day = day.clone();
        FileLog {
            base: path,
            mode,
            max_bytes,
            keep,
            state: Mutex::new(None),
            clock: Arc::new(move || *clock_day.lock().expect("day")),
        }
    }

    fn files_in(dir: &Path) -> Vec<String> {
        let mut names = fs::read_dir(dir)
            .expect("read dir")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn size_rotation_keeps_two_files() {
        let dir = temp_dir("size");
        let path = dir.join("kova.log");
        let day = Arc::new(Mutex::new((2026, 11, 1)));
        let sink = sink_on(path, Rotate::Size, 80, 2, day);
        for index in 0..8 {
            let record = LogRecord::new(Level::Info, format!("row-{index:02}-0123456789abcdef"));
            sink.emit(&record);
        }
        let names = files_in(&dir);
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(names.iter().any(|name| name == "kova.log"), "{names:?}");
        assert!(names.iter().any(|name| name == "kova.log.1"), "{names:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn date_rotation_opens_a_file_per_day() {
        let dir = temp_dir("date");
        let path = dir.join("kova.log");
        let day = Arc::new(Mutex::new((2026, 11, 1)));
        let sink = sink_on(path, Rotate::Date, 10_000, 7, day.clone());
        sink.emit(&LogRecord::new(Level::Info, "monday"));
        *day.lock().expect("day") = (2026, 11, 2);
        sink.emit(&LogRecord::new(Level::Info, "tuesday"));
        let names = files_in(&dir);
        assert_eq!(
            names,
            vec![
                "kova-2026-11-01.log".to_string(),
                "kova-2026-11-02.log".to_string(),
            ]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn date_and_size_keeps_the_newest_files() {
        let dir = temp_dir("both");
        let path = dir.join("kova.log");
        let day = Arc::new(Mutex::new((2026, 11, 1)));
        let sink = sink_on(path, Rotate::DateAndSize, 70, 2, day.clone());
        for index in 0..6 {
            sink.emit(&LogRecord::new(
                Level::Info,
                format!("row-{index:02}-0123456789abcdef"),
            ));
        }
        *day.lock().expect("day") = (2026, 11, 2);
        sink.emit(&LogRecord::new(Level::Info, "next-day-0123456789abcdef"));
        let names = files_in(&dir);
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(
            names.iter().any(|name| name.contains("2026-11-02")),
            "{names:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
