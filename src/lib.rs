//! Kova engine library.
//!
//! The `kova` binary calls [`serve`]. The unixODBC driver is the same
//! crate built as `libkova.so`; its entry points live in [`odbc`].

mod config;
pub mod dump;
mod index;
mod logger;
mod metrics;
mod network;
pub mod odbc;
mod protocol;
mod query_engine;
mod replication;
mod search;
mod security;
mod shard;
mod storage;
mod types;

use std::io::Write;
use std::panic;

/// Number of storage partitions. Phase 2 uses a small fixed set; later
/// phases will size this from the available core count.
const PARTITION_COUNT: usize = 4;

/// Writes a panic to stderr in the same line shape as the logger.
pub fn install_panic_hook() {
    panic::set_hook(Box::new(|info| {
        let msg = match info.payload().downcast_ref::<&str>() {
            Some(text) => text.to_string(),
            None => match info.payload().downcast_ref::<String>() {
                Some(text) => text.clone(),
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

/// Starts the network server. A replication failure or a bind failure
/// ends the process.
pub async fn serve() {
    let config = &config::CONFIG;
    logger::LOGGER.set_min_level(config.log_level());

    logger::LOGGER.info(format!(
        "Kova engine starting (port {}, log level {}, data_dir {}, wal_fsync={}, warn_on_non_mongo_queries={})",
        config.port,
        config.log_level(),
        config.storage.data_dir,
        config.storage.wal_fsync,
        config.warn_on_non_mongo_queries
    ));

    let router = storage::PartitionRouter::new(PARTITION_COUNT);
    let shards = if config.sharding.enabled && config.sharding.role == "router" {
        logger::LOGGER.info(format!(
            "Shard router hashing _id across {} shard(s)",
            config.sharding.shards.len()
        ));
        Some(std::sync::Arc::new(shard::ShardRouter::connect(
            &config.sharding.shards,
        )))
    } else {
        if config.sharding.enabled {
            logger::LOGGER.info(format!(
                "Shard {} storing the documents the router sends",
                config.sharding.id
            ));
        }
        None
    };
    let replica = if config.replication.enabled {
        let applier = network::storage_applier(router.clone());
        match replication::boot(config, applier).await {
            Ok(replica) => Some(std::sync::Arc::new(replica)),
            Err(err) => {
                logger::LOGGER.error(format!("Replication failed to start: {}", err));
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    if let Err(err) = network::start_server(router, replica, shards).await {
        logger::LOGGER.error(format!("Network server error: {}", err));
        std::process::exit(1);
    }
}
