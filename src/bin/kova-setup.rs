fn main() {
    if let Err(err) = kova::setup::run() {
        eprintln!("kova-setup: {err}");
        std::process::exit(1);
    }
}
