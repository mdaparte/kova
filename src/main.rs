#[tokio::main]
async fn main() {
    kova::install_panic_hook();
    kova::serve().await;
}
