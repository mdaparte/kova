#!/bin/sh
# Build iggy-server from the Apache Iggy tag whose Rust SDK is crates.io iggy 0.7.0.
# That tag is server-0.5.0. The later tag named server-0.7.0 speaks a newer protocol.
# The checkout stays in target/iggy-src so the next build does not start over.
set -eu

tag=server-0.5.0
out=${1:?output directory}
mkdir -p "$out"

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
# cargo names the Windows binary iggy-server.exe. The other platforms do not.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) exe=.exe ;;
  *) exe= ;;
esac
bin="$out/iggy-server$exe"
tagfile="$out/iggy-server.tag"

# A binary already built for this tag is enough. Delete it to force a rebuild.
if [ -x "$bin" ] && [ ! -f "$tagfile" ]; then
  printf '%s\n' "$tag" > "$tagfile"
fi
if [ -x "$bin" ] && [ "$(cat "$tagfile")" = "$tag" ]; then
  printf '%s\n' "iggy-server $tag is already built."
  exit 0
fi

cache="$root/target/iggy-src"
src="$cache/src"
mkdir -p "$cache"
if [ ! -d "$src/.git" ]; then
  rm -rf "$src"
  git clone --depth 1 --branch "$tag" https://github.com/apache/iggy.git "$src"
fi

(
  cd "$src"
  # The warnings are in the Iggy tree. Allow them so this log stays on errors.
  RUSTFLAGS="${RUSTFLAGS:-} -A warnings" cargo build --release --locked -p server --bin iggy-server
)

cp "$src/target/release/iggy-server$exe" "$bin"
chmod 755 "$bin"
printf '%s\n' "$tag" > "$tagfile"
cp "$src/NOTICE" "$out/NOTICE-iggy"
cp "$src/LICENSE" "$out/LICENSE-iggy"
if [ -f "$src/DEPENDENCIES" ]; then
  cp "$src/DEPENDENCIES" "$out/DEPENDENCIES-iggy"
fi
