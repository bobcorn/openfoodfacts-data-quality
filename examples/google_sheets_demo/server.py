from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast

from examples.google_sheets_demo.api import (
    build_public_demo_config,
    clear_validation_output_request,
    mock_upload_request,
    parse_csv_request,
    prepare_upload_candidates_request,
    validate_request,
)

PORT_ENV_VAR = "GOOGLE_SHEETS_DEMO_PORT"
DEFAULT_PORT = 8501
MAX_JSON_BODY_BYTES = 10 * 1024 * 1024
STATIC_ROOT = Path(__file__).with_name("web")
JsonDict = dict[str, object]
LOGGER = logging.getLogger(__name__)
CONTENT_SECURITY_POLICY = "; ".join(
    (
        "default-src 'self'",
        "base-uri 'none'",
        "object-src 'none'",
        "frame-ancestors 'none'",
        "script-src 'self' https://accounts.google.com https://apis.google.com",
        "style-src 'self' 'unsafe-inline' https://accounts.google.com",
        "img-src 'self' data: https:",
        (
            "connect-src 'self' https://sheets.googleapis.com "
            "https://accounts.google.com https://www.googleapis.com "
            "https://content.googleapis.com"
        ),
        (
            "frame-src https://accounts.google.com https://docs.google.com "
            "https://drive.google.com https://*.googleusercontent.com"
        ),
        "form-action 'self' https://accounts.google.com",
    )
)


class DemoRequestHandler(BaseHTTPRequestHandler):
    """Serve the local browser app and its small JSON API."""

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._write_text_response(HTTPStatus.OK, "ok")
            return
        if self.path == "/api/config":
            self._write_json_response(
                HTTPStatus.OK,
                build_public_demo_config(self._demo_server.environ).to_payload(),
            )
            return
        if self.path == "/" or self.path == "/index.html":
            self._serve_static_file("index.html", "text/html; charset=utf-8")
            return
        if self.path == "/static/app.js":
            self._serve_static_file("app.js", "text/javascript; charset=utf-8")
            return
        if self.path == "/static/style.css":
            self._serve_static_file("style.css", "text/css; charset=utf-8")
            return
        self._write_json_response(
            HTTPStatus.NOT_FOUND,
            {"error": f"Unknown route: {self.path}"},
        )

    def do_POST(self) -> None:  # noqa: N802
        try:
            payload = self._read_json_request()
            response_payload = self._dispatch_post(self.path, payload)
        except ValueError as error:
            self._write_json_response(
                HTTPStatus.BAD_REQUEST,
                {"error": str(error)},
            )
            return
        except Exception:  # noqa: BLE001
            LOGGER.exception("Unexpected Google Sheets demo server error.")
            self._write_json_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "Unexpected server error."},
            )
            return

        self._write_json_response(HTTPStatus.OK, response_payload)

    def log_message(self, format: str, *args: object) -> None:
        super().log_message(format, *args)

    @property
    def _demo_server(self) -> DemoHttpServer:
        return cast(DemoHttpServer, self.server)

    def _dispatch_post(
        self,
        path: str,
        payload: JsonDict,
    ) -> JsonDict:
        handlers: dict[str, Callable[[JsonDict], JsonDict]] = {
            "/api/parse-csv": parse_csv_request,
            "/api/validate": validate_request,
            "/api/clear-validation-output": clear_validation_output_request,
            "/api/prepare-upload-candidates": prepare_upload_candidates_request,
            "/api/mock-upload": mock_upload_request,
        }
        handler = handlers.get(path)
        if handler is None:
            raise ValueError(f"Unknown route: {path}")
        return handler(payload)

    def _read_json_request(self) -> dict[str, object]:
        content_length = self.headers.get("Content-Length", "0")
        try:
            size = int(content_length)
        except ValueError as error:
            raise ValueError("The request body length is invalid.") from error
        if size < 0:
            raise ValueError("The request body length is invalid.")
        if size > MAX_JSON_BODY_BYTES:
            raise ValueError("The request body is too large for this demo app.")
        raw_body = self.rfile.read(size) if size > 0 else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError("The request body must be valid JSON.") from error
        if not isinstance(payload, dict):
            raise ValueError("The request body must be one JSON object.")
        return cast(JsonDict, payload)

    def _serve_static_file(self, filename: str, content_type: str) -> None:
        target_path = STATIC_ROOT / filename
        if not target_path.exists():
            self._write_json_response(
                HTTPStatus.NOT_FOUND,
                {"error": f"Missing static file: {filename}"},
            )
            return
        self.send_response(HTTPStatus.OK)
        self._send_common_headers(content_type=content_type)
        self.end_headers()
        self.wfile.write(target_path.read_bytes())

    def _write_text_response(self, status: HTTPStatus, text: str) -> None:
        self.send_response(status)
        self._send_common_headers(content_type="text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(text.encode("utf-8"))

    def _write_json_response(
        self,
        status: HTTPStatus,
        payload: dict[str, object] | dict[str, str],
    ) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self._send_common_headers(content_type="application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def _send_common_headers(self, *, content_type: str) -> None:
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Security-Policy", CONTENT_SECURITY_POLICY)
        self.send_header(
            "Permissions-Policy",
            "camera=(), geolocation=(), microphone=(), usb=()",
        )
        self.send_header("Cross-Origin-Opener-Policy", "same-origin-allow-popups")


class DemoHttpServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        RequestHandlerClass: type[BaseHTTPRequestHandler],  # noqa: N803
        environ: dict[str, str],
    ) -> None:
        self.environ = environ
        super().__init__(server_address, RequestHandlerClass)


def create_server(
    *,
    host: str,
    port: int,
    environ: dict[str, str] | None = None,
) -> DemoHttpServer:
    _ = (DemoRequestHandler.do_GET, DemoRequestHandler.do_POST)
    server = DemoHttpServer(
        (host, port),
        DemoRequestHandler,
        environ=dict(environ or os.environ),
    )
    return server


def main() -> None:
    port = int(os.environ.get(PORT_ENV_VAR, str(DEFAULT_PORT)))
    server = create_server(host="0.0.0.0", port=port)
    print(f"Google Sheets demo listening on http://0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
