"""Repo-level security checks (docs/09 S1, S5, S7, S8, S10, S12); input validation lives in test_api.py."""

import ast
import asyncio
import re
import subprocess
from pathlib import Path

from websockets.asyncio.server import serve

from ingest.aisstream import MAX_MESSAGE_BYTES, run
from tests.unit.test_aisstream import BBOX, FakeSender

ROOT = Path(__file__).resolve().parents[2]
SOURCE = [p for p in ROOT.rglob("*.py") if ".venv" not in p.parts and "tests" not in p.parts]


def test_no_secret_value_in_tracked_files():
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    assert ".env" not in tracked
    for name in tracked:
        text = (ROOT / name).read_text(errors="ignore") if (ROOT / name).is_file() else ""
        assert not re.search(r"AISSTREAM_API_KEY[ \t]*=[ \t]*[0-9A-Za-z]{16,}", text), name


def test_compose_publishes_only_on_loopback():
    candidates = [
        ROOT / "docker-compose.yml",
        ROOT / "infra/docker-compose.yml",
        ROOT.parent / "infra/docker-compose.yml",
    ]
    compose_file = next((p for p in candidates if p.is_file()), None)
    assert compose_file is not None, "docker-compose.yml not found"
    text = compose_file.read_text()
    published = re.findall(r'^\s+- "?([\d.]*:?\d+:\d+)"?\s*$', text, re.M)
    assert published
    assert all(p.startswith("127.0.0.1:") for p in published), published


def test_every_outbound_http_call_has_a_timeout():
    for path in SOURCE:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "urlopen":
                assert any(k.arg == "timeout" for k in node.keywords), f"{path.name}:{node.lineno}"


def test_dashboard_never_puts_api_data_in_innerhtml():
    candidates = [
        ROOT / "dashboard/static/app.js",
        ROOT.parent / "frontend/src/app.js",
        ROOT / "frontend/src/app.js",
    ]
    js_file = next((p for p in candidates if p.is_file()), None)
    if js_file is None:
        return
    js = js_file.read_text()
    uses = re.findall(r"innerHTML\s*=\s*([^;]+);", js)
    # the one constant legend icon (a type name from our own list and two numbers); the rest uses textContent
    assert uses == ["shipSvg('cargo', 8, 16)"]
    assert not re.search(r"insertAdjacentHTML|outerHTML|document\.write", js)


def test_oversized_websocket_frame_is_dropped_not_fatal():
    async def handler(ws):
        await ws.recv()
        await ws.send("x" * (MAX_MESSAGE_BYTES + 1))  # closes the connection with 1009
        await asyncio.sleep(1)

    async def scenario():
        server = await serve(handler, "127.0.0.1", 0, max_size=None)
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        sender = FakeSender()
        try:
            await asyncio.wait_for(run(url, "k", BBOX, sender, stop_after=1, max_backoff=0.05), 0.5)
        except TimeoutError:
            pass  # still retrying after the drop, which is the point
        finally:
            server.close()
        return sender

    assert asyncio.run(scenario()).sent == []
