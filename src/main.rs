mod config;
mod logger;
mod network;
mod protocol;
mod search;
mod storage;
mod types;

use logger::LOGGER;

/// Number of storage partitions. Phase 2 uses a small fixed set; later
/// phases will size this from the available core count.
const PARTITION_COUNT: usize = 4;

#[tokio::main]
async fn main() {
    // Resolve configuration (file + environment) before anything
    // logs, so the level filter applies from the very first record.
    let config = &config::CONFIG;
    LOGGER.set_min_level(config.log_level());

    LOGGER.info(format!(
        "Kova engine starting (port {}, log level {}, warn_on_non_mongo_queries={})",
        config.port, config.log_level, config.warn_on_non_mongo_queries
    ));

    let router = storage::PartitionRouter::new(PARTITION_COUNT);

    if let Err(err) = network::start_server(router).await {
        LOGGER.error(format!("Network server error: {}", err));
        std::process::exit(1);
    }
}
