# Kova

Kova is a database you talk to with a MongoDB driver. One process stores the documents, answers SQL, and searches text. The listener defaults to port 27018.

A merge into `main` publishes the Linux x86_64 build and a container image.

Install on the machine, then start from a directory you want to keep:

```sh
curl -fsSL -o install.sh https://github.com/mdaparte/kova/releases/latest/download/install.sh
sh install.sh
KOVA_CONFIG="$HOME/.local/share/kova/kova.toml" kova
```

Or run the image. The volume keeps the pages and the log:

```sh
docker run --rm -p 27018:27018 -v kova:/var/lib/kova ghcr.io/mdaparte/kova:latest
```

Configuration is `kova.toml`. Security is off unless `[security]` is enabled. `kova-setup` writes that file and opens again later with the current values. The suggestion is security closed. Leaving it open requires typing `OPEN`. `kova-setup --window` asks the same questions in a window. The installed `seed-oracle.js` loads a sample into the database `db`.

The same install puts `iggy-server` on `PATH`. It is Apache Iggy tag `server-0.5.0`, the server that matches the client inside `kova`. It listens on `127.0.0.1:8090`. Kova does not start it. From a checkout, `make iggy-server` builds it and `make run-iggy` starts it from `[feed.server]` in `kova.toml`. The feed stays off until `[feed]` is enabled.

The end-user guide is the static site in [`site/`](site/index.html). Start at [Getting started](site/docs/getting-started.html).

`kovadump backup --db db db.kdmp` writes a backup of a running server. `kovadump restore --db db db.kdmp` reads it back.

People changing Kova build from this repository with the Makefile (`make release`, `make test`). `make doc` builds the API reference.

The crates.io package name is `kovadb`. The library name stays `kova`. After 0.1.0 is published, `cargo install kovadb` installs the `kova` and `kovadump` binaries. A Rust project depends on the package and calls the library:

```toml
kovadb = "0.1"
```

```rust
#[tokio::main]
async fn main() {
    kova::serve().await;
}
```

0.1.0 is published from GitHub Actions when a merge into `main` changes the crate sources. It is not on crates.io yet.

Kova is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
