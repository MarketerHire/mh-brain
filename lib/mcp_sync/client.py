"""MCP HTTP client — JSON-RPC 2.0 over HTTP with SSE response parsing.

Reuses the proven _call_mcp() wire format from mh1-hq/lib/retrieval/mcp_proxy.py
but as a standalone class with retry and rate-limit support.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2
_RETRY_BACKOFF = (5, 15)


class MCPClient:
    """Thin HTTP client for calling MCP tools via JSON-RPC 2.0."""

    def __init__(self, url: str, auth_token: str, default_timeout: int = 120):
        if not url:
            raise ValueError("MCP url is required")
        self.url = url
        self.auth_token = auth_token
        self.default_timeout = default_timeout

    def call(self, tool_name: str, arguments: dict,
             timeout: int | None = None) -> Any:
        """Call an MCP tool and return the parsed result."""
        timeout = timeout or self.default_timeout

        for attempt in range(1 + _MAX_RETRIES):
            try:
                return self._do_call(tool_name, arguments, timeout)
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                if attempt < _MAX_RETRIES:
                    wait = _RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)]
                    logger.warning(
                        "MCP call %s failed (attempt %d/%d): %s — retrying in %ds",
                        tool_name, attempt + 1, _MAX_RETRIES + 1, exc, wait,
                    )
                    time.sleep(wait)
                else:
                    raise

    def _do_call(self, tool_name: str, arguments: dict,
                 timeout: int) -> Any:
        payload = json.dumps({
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
            "id": int(time.time() * 1000),
        }).encode()

        req = Request(self.url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json, text/event-stream")
        if self.auth_token:
            req.add_header("Authorization", f"Bearer {self.auth_token}")

        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode()

        return self._parse_response(body, tool_name)

    @staticmethod
    def _parse_response(body: str, tool_name: str) -> Any:
        """Parse SSE data: frames first, fall back to plain JSON."""
        result = None

        for line in body.splitlines():
            if not line.startswith("data:"):
                continue
            try:
                frame = json.loads(line[5:])
                if "result" not in frame:
                    continue
                for content_block in frame["result"].get("content", []):
                    text = content_block.get("text", "")
                    if not text:
                        continue
                    try:
                        result = json.loads(text)
                    except json.JSONDecodeError:
                        result = text
            except json.JSONDecodeError:
                continue

        if result is not None:
            return result

        try:
            frame = json.loads(body)
            if "result" in frame:
                for content_block in frame["result"].get("content", []):
                    text = content_block.get("text", "")
                    if not text:
                        continue
                    try:
                        result = json.loads(text)
                    except json.JSONDecodeError:
                        result = text
        except json.JSONDecodeError:
            pass

        if result is None:
            logger.warning("MCP %s returned no parseable result", tool_name)

        return result
