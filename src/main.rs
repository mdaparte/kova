mod logger;
mod network;

use logger::LOGGER;

#[tokio::main]
async fn main() {
    LOGGER.info("Kova network server starting");

    if let Err(err) = network::start_server().await {
        LOGGER.error(&format!("Network server error: {}", err));
        std::process::exit(1);
    }
}
