mod config;
mod index;
mod logger;
mod network;
mod protocol;
mod query_engine;
mod search;
mod storage;
mod types;

use std::io::Write;
use std::panic;

fn register_panic_hook() {
    panic::set_hook(Box::new(|info| {
        let msg = match info.payload().downcast_ref::<&str>() {
            Some(s) => s.to_string(),
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => s.clone(),
                None => "panic without message".to_string(),
            },
        };
        let location = if let Some(loc) = info.location() {
            format!(" at {}:{}:{}", loc.file(), loc.line(), loc.column())
        } else {
            String::new()
        };
        let stamp = logger::format_timestamp(logger::now_millis());
        let mut buf = Vec::with_capacity(256);
        buf.extend_from_slice(b"[Kova] ");
        buf.extend_from_slice(stamp.as_bytes());
        buf.extend_from_slice(b" ERROR - panic");
        buf.extend_from_slice(location.as_bytes());
        buf.extend_from_slice(b": ");
        buf.extend_from_slice(msg.as_bytes());
        buf.push(b'\n');
        let _ = std::io::stderr().write_all(&buf);
        let _ = std::io::stderr().flush();
    }));
}

use logger::LOGGER;

/// Number of storage partitions. Phase 2 uses a small fixed set; later
/// phases will size this from the available core count.
const PARTITION_COUNT: usize = 4;

#[tokio::main]
async fn main() {
    register_panic_hook();

    // Resolve configuration (file + environment) before anything
    // logs, so the level filter applies from the very first record.
    let config = &config::CONFIG;
    LOGGER.set_min_level(config.log_level());

    LOGGER.info(format!(
        "Kova engine starting (port {}, log level {}, data_dir {}, wal_fsync={}, warn_on_non_mongo_queries={})",
        config.port,
        config.log_level,
        config.storage.data_dir,
        config.storage.wal_fsync,
        config.warn_on_non_mongo_queries
    ));

    let router = storage::PartitionRouter::new(PARTITION_COUNT);

    if let Err(err) = network::start_server(router).await {
        LOGGER.error(format!("Network server error: {}", err));
        std::process::exit(1);
    }
}
