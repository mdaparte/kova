#!/usr/bin/env python3
"""Turn [feed] and [feed.server] in kova.toml into an Iggy server file.

Prints shell exports. make run-iggy evals them. Kova itself does not
start iggy-server.
"""

import os
import sys
import tomllib
from pathlib import Path


def sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def main() -> None:
    raw = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("KOVA_CONFIG", "kova.toml")
    config_path = Path(raw)
    if not config_path.is_file():
        sys.stderr.write(f"missing {config_path}\n")
        sys.exit(1)
    with config_path.open("rb") as handle:
        doc = tomllib.load(handle)
    feed = doc.get("feed") or {}
    server = feed.get("server") or {}
    base = config_path.resolve().parent
    data = Path(str(server.get("path") or "iggy-data"))
    if not data.is_absolute():
        data = (base / data).resolve()
    address = str(server.get("address") or feed.get("address") or "127.0.0.1:8090")
    feed_address = str(feed.get("address") or "127.0.0.1:8090")
    log_level = str(server.get("log_level") or "info")
    if log_level not in ("debug", "info", "warn", "error"):
        sys.stderr.write(
            f"feed.server.log_level {log_level!r} must be debug, info, warn, or error\n"
        )
        sys.exit(1)
    http = bool(server.get("http_enabled", False))
    quic = bool(server.get("quic_enabled", False))
    data.mkdir(parents=True, exist_ok=True)
    iggy_toml = data / "server.toml"
    iggy_toml.write_text(
        "\n".join(
            [
                "[system]",
                f'path = "{data.as_posix()}"',
                "",
                "[system.logging]",
                f'level = "{log_level}"',
                f'path = "{(data / "logs").as_posix()}"',
                "",
                "[tcp]",
                "enabled = true",
                f'address = "{address}"',
                "",
                "[http]",
                f"enabled = {'true' if http else 'false'}",
                'address = "127.0.0.1:3000"',
                "",
                "[quic]",
                f"enabled = {'true' if quic else 'false'}",
                'address = "127.0.0.1:8080"',
                "",
            ]
        )
    )
    if address != feed_address:
        sys.stderr.write(f"iggy listens on {address}. [feed] address is {feed_address}.\n")
    user = str(feed.get("username") or "")
    password = str(feed.get("password") or "")
    print(f"export IGGY_CONFIG_PATH={sh_quote(str(iggy_toml))}")
    if user and password:
        print(f"export IGGY_ROOT_USERNAME={sh_quote(user)}")
        print(f"export IGGY_ROOT_PASSWORD={sh_quote(password)}")
    elif user or password:
        sys.stderr.write("[feed] username and password must both be set, or both be empty\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
