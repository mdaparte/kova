#!/bin/sh
# Start the iggy-server built by `make iggy-server`.
# Listen address, data directory, and log level come from [feed.server].
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
bin=${IGGY_BIN:-"$root/target/iggy-server"}
if [ ! -x "$bin" ]; then
  printf '%s\n' "iggy-server is not built. Run: make iggy-server" >&2
  exit 1
fi
config=${KOVA_CONFIG:-"$root/kova.toml"}
eval "$(python3 "$root/packaging/iggy-server-config.py" "$config")"
python3 - "$IGGY_CONFIG_PATH" << 'PY'
import socket
import sys
import tomllib
from pathlib import Path

path = Path(sys.argv[1])
address = tomllib.loads(path.read_text())["tcp"]["address"]
host, port = address.rsplit(":", 1)
if host in ("0.0.0.0", "::", ""):
    host = "127.0.0.1"
probe = socket.socket()
probe.settimeout(0.3)
in_use = probe.connect_ex((host, int(port))) == 0
probe.close()
if in_use:
    sys.stderr.write(
        f"{address} is already in use. Change [feed] address and [feed.server] address.\n"
    )
    sys.exit(1)
PY
exec "$bin"
