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

Configuration is `kova.toml`. Security is off unless `[security]` is enabled. The installed `seed-oracle.js` loads a sample into the database `db`.

The end-user guide is the static site in [`site/`](site/index.html). Start at [Getting started](site/docs/getting-started.html).

`kovadump backup --db db db.kdmp` writes a backup of a running server. `kovadump restore --db db db.kdmp` reads it back.

People changing Kova build from this repository with the Makefile (`make release`, `make test`). `make doc` builds the API reference.

Kova is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
