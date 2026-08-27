#!/usr/bin/env python3
"""Serve live playground HTML while preserving an existing in-memory backend."""

from __future__ import annotations

import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--html", required=True, type=Path)
    parser.add_argument("--backend-port", required=True, type=int)
    parser.add_argument("--port", required=True, type=int)
    return parser.parse_args()


def make_handler(html_path: Path, backend_port: int) -> type[BaseHTTPRequestHandler]:
    class PlaygroundHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802
            if urlsplit(self.path).path in ("/", "/index.html"):
                body = html_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                return
            self._proxy()

        def do_POST(self) -> None:  # noqa: N802
            self._proxy()

        def _proxy(self) -> None:
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length) if content_length else None
            headers = {}
            if content_type := self.headers.get("Content-Type"):
                headers["Content-Type"] = content_type

            connection = http.client.HTTPConnection("127.0.0.1", backend_port, timeout=5)
            try:
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                response_body = response.read()
                self.send_response(response.status)
                self.send_header(
                    "Content-Type",
                    response.getheader("Content-Type", "application/octet-stream"),
                )
                self.send_header("Content-Length", str(len(response_body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(response_body)
            except OSError as error:
                response_body = f"backend unavailable: {error}".encode()
                self.send_response(502)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(response_body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(response_body)
            finally:
                connection.close()

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return PlaygroundHandler


def main() -> None:
    args = parse_args()
    html_path = args.html.resolve(strict=True)
    server = ThreadingHTTPServer(
        ("127.0.0.1", args.port),
        make_handler(html_path, args.backend_port),
    )
    print(
        f"serving {html_path.name} at http://127.0.0.1:{args.port} "
        f"with API backend 127.0.0.1:{args.backend_port}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
