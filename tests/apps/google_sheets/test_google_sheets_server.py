from __future__ import annotations

import http.client
from contextlib import closing
from threading import Thread

from apps.google_sheets.server import create_server


def test_google_sheets_server_supports_head_routes() -> None:
    root_response, root_body = _request("HEAD", "/")
    assert root_response.status == 200
    assert root_body == b""

    health_response, health_body = _request("HEAD", "/healthz")
    assert health_response.status == 200
    assert health_body == b""


def test_google_sheets_server_renders_shared_ui_shell() -> None:
    response, body = _request("GET", "/")
    decoded_body = body.decode("utf-8")
    assert response.status == 200
    assert '<main class="page-frame">' in decoded_body
    assert '<div class="page-stack">' in decoded_body
    assert '<section class="surface-shell workflow-shell shell">' in decoded_body
    assert '<div class="page-meta">' not in decoded_body
    assert "Google Sheets Workflow" in decoded_body


def _request(
    method: str,
    path: str,
) -> tuple[http.client.HTTPResponse, bytes]:
    host = "127.0.0.1"
    server = create_server(host=host, port=0, environ={})
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with closing(
            http.client.HTTPConnection(host, server.server_port, timeout=5)
        ) as client:
            client.request(method, path)
            response = client.getresponse()
            body = response.read()
        return response, body
    finally:
        server.shutdown()
        worker.join(timeout=5)
        server.server_close()
