mod logger;
mod network;
mod protocol;
mod storage;
mod types;

use logger::LOGGER;

/// Number of storage partitions. Phase 2 uses a small fixed set; later
/// phases will size this from the available core count.
const PARTITION_COUNT: usize = 4;

#[tokio::main]
async fn main() {
    LOGGER.info("Kova network server starting");

    let router = storage::PartitionRouter::new(PARTITION_COUNT);

    if let Err(err) = network::start_server(router).await {
        LOGGER.error(format!("Network server error: {}", err));
        std::process::exit(1);
    }
}
