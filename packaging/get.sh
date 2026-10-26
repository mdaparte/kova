#!/bin/sh
# Download the current public Linux build and install it.
# Published as install.sh on the GitHub release.
set -eu

repo=${KOVA_REPO:-mdaparte/kova}
asset=kova-x86_64-unknown-linux-gnu.tar.gz
url="https://github.com/${repo}/releases/latest/download/${asset}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

curl -fsSL -o "$tmp/$asset" "$url"
tar -xzf "$tmp/$asset" -C "$tmp"
"$tmp/kova-x86_64-unknown-linux-gnu/install.sh"
