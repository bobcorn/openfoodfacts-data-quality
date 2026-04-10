from __future__ import annotations

import argparse
import subprocess
import time
from http.client import RemoteDisconnected
from urllib.error import URLError
from urllib.request import urlopen


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one local smoke test against a built Docker image."
    )
    parser.add_argument("--image", required=True, help="Docker image tag to run.")
    parser.add_argument(
        "--host-port",
        type=int,
        required=True,
        help="Host port published for the smoke test.",
    )
    parser.add_argument(
        "--container-port",
        type=int,
        required=True,
        help="Container port exposed by the image.",
    )
    parser.add_argument(
        "--path",
        default="/",
        help="HTTP path to probe on the running container.",
    )
    parser.add_argument(
        "--expect-text",
        default="",
        help="Substring that must appear in the response body.",
    )
    parser.add_argument(
        "--secondary-path",
        default="",
        help="Optional second HTTP path to probe after the main path succeeds.",
    )
    parser.add_argument(
        "--secondary-expect-text",
        default="",
        help="Optional substring that must appear in the second response body.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=180,
        help="Maximum time to wait for the image to start serving.",
    )
    args = parser.parse_args()

    publish_spec = f"127.0.0.1:{args.host_port}:{args.container_port}"
    container_id = subprocess.check_output(
        ["docker", "run", "-d", "-p", publish_spec, args.image],
        text=True,
    ).strip()
    succeeded = False
    try:
        _wait_for_http_response(
            host_port=args.host_port,
            path=args.path,
            expect_text=args.expect_text,
            timeout_seconds=args.timeout_seconds,
        )
        if args.secondary_path:
            _wait_for_http_response(
                host_port=args.host_port,
                path=args.secondary_path,
                expect_text=args.secondary_expect_text,
                timeout_seconds=args.timeout_seconds,
            )
        succeeded = True
    finally:
        if not succeeded:
            subprocess.run(["docker", "logs", container_id], check=False)
        subprocess.run(
            ["docker", "rm", "-f", container_id],
            check=False,
            capture_output=True,
            text=True,
        )
    return 0


def _wait_for_http_response(
    *,
    host_port: int,
    path: str,
    expect_text: str,
    timeout_seconds: int,
) -> None:
    deadline = time.time() + timeout_seconds
    url = f"http://127.0.0.1:{host_port}{path}"
    last_error: str | None = None
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=5) as response:
                body = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                last_error = f"{url} returned status {response.status}."
                time.sleep(2)
                continue
            if expect_text and expect_text not in body:
                last_error = f"{url} did not include the expected text."
                time.sleep(2)
                continue
            return
        except (URLError, RemoteDisconnected, TimeoutError, ConnectionError) as error:
            last_error = str(error)
            time.sleep(2)

    message = last_error or f"{url} did not become ready in time."
    raise RuntimeError(message)


if __name__ == "__main__":
    raise SystemExit(main())
