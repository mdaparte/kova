#!/bin/sh
# Build iggy-server from the Apache Iggy tag whose Rust SDK is crates.io iggy 0.7.0.
# That tag is server-0.5.0. The later tag named server-0.7.0 speaks a newer protocol.
set -eu

tag=server-0.5.0
out=${1:?output directory}
mkdir -p "$out"

src=$(mktemp -d)
trap 'rm -rf "$src"' EXIT

git clone --depth 1 --branch "$tag" https://github.com/apache/iggy.git "$src"
(
  cd "$src"
  cargo build --release --locked -p server --bin iggy-server
)

cp "$src/target/release/iggy-server" "$out/iggy-server"
chmod 755 "$out/iggy-server"
cp "$src/NOTICE" "$out/NOTICE-iggy"
cp "$src/LICENSE" "$out/LICENSE-iggy"
if [ -f "$src/DEPENDENCIES" ]; then
  cp "$src/DEPENDENCIES" "$out/DEPENDENCIES-iggy"
fi
