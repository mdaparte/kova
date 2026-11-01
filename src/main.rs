fn main() {
    #[cfg(windows)]
    if std::env::args().any(|arg| arg == "--service") {
        kova::install_panic_hook();
        if let Err(err) = kova::setup::run_kova_service() {
            eprintln!("{err}");
            std::process::exit(1);
        }
        return;
    }
    runtime_main()
}

#[tokio::main]
async fn runtime_main() {
    kova::install_panic_hook();
    kova::serve().await;
}
