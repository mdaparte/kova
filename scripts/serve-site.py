#!/usr/bin/env python3
"""Serve site/ the way Vercel does: /docs is docs/index.html, and a path
without .html maps to the matching file. Bind to 127.0.0.1."""

import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "site"


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        rel = path.lstrip("/")
        if path not in ("/", "") and path.endswith("/"):
            self.send_response(308)
            self.send_header("Location", path.rstrip("/") or "/")
            self.end_headers()
            return
        target = ROOT / rel
        if target.is_dir():
            self.path = "/" + rel.rstrip("/") + "/index.html"
        elif rel and not target.exists() and (ROOT / (rel + ".html")).is_file():
            self.path = "/" + rel + ".html"
        return super().do_GET()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 4173
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Kova site at http://127.0.0.1:{port}/", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
