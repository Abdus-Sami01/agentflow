from __future__ import annotations

import ipaddress
import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


ALLOWED_SCHEMES = {"https"}
BLOCKED_HOST_SUFFIXES = {".local", ".internal", ".localdomain"}
CLOUD_METADATA_HOSTS = {
    "169.254.169.254",
    "metadata.google.internal",
    "metadata.goog",
    "100.100.100.200",
}
MAX_RESPONSE_BYTES = 5_000_000
MAX_REDIRECTS = 3


class SSRFBlocked(Exception):
    pass


def _resolve_ips(hostname: str) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    infos = socket.getaddrinfo(hostname, None)
    addrs = []
    for info in infos:
        raw = info[4][0]
        try:
            addrs.append(ipaddress.ip_address(raw))
        except ValueError:
            continue
    return addrs


def check_url_safety(url: str, allowed_schemes: set[str] | None = None, allow_private: bool = False) -> None:
    schemes = allowed_schemes or ALLOWED_SCHEMES
    parsed = urllib.parse.urlparse(url)

    if parsed.scheme not in schemes:
        raise SSRFBlocked(f"scheme {parsed.scheme!r} not allowed (allowed: {sorted(schemes)})")

    host = parsed.hostname
    if not host:
        raise SSRFBlocked("URL has no hostname")

    host_lower = host.lower()

    if host_lower in CLOUD_METADATA_HOSTS:
        raise SSRFBlocked(f"host {host!r} is a cloud metadata endpoint")

    for suffix in BLOCKED_HOST_SUFFIXES:
        if host_lower.endswith(suffix):
            raise SSRFBlocked(f"host {host!r} uses a blocked internal suffix")

    if allow_private:
        return

    try:
        resolved = _resolve_ips(host)
    except socket.gaierror as e:
        raise SSRFBlocked(f"cannot resolve host {host!r}: {e}")

    if not resolved:
        raise SSRFBlocked(f"host {host!r} resolved to no addresses")

    for ip in resolved:
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            raise SSRFBlocked(f"host {host!r} resolves to non-public address {ip}")


class HTTPNode(BaseNode):
    def __init__(
        self,
        name: str,
        url: str = "",
        method: str = "GET",
        headers: dict[str, str] | None = None,
        timeout_s: float = 10.0,
        parse_json: bool = True,
        allowed_schemes: set[str] | None = None,
        allow_private: bool = False,
        max_bytes: int = MAX_RESPONSE_BYTES,
        url_fn: Callable[[dict[str, Any], SharedContext], str] | None = None,
        **config,
    ):
        super().__init__(name, **config)
        self._url = url
        self._method = method.upper()
        self._headers = headers or {}
        self._timeout = timeout_s
        self._parse_json = parse_json
        self._allowed_schemes = allowed_schemes or ALLOWED_SCHEMES
        self._allow_private = allow_private
        self._max_bytes = max_bytes
        self._url_fn = url_fn

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        try:
            url = self._url_fn(inputs, context) if self._url_fn else self._url
        except Exception as e:
            return NodeOutput(error=f"url builder failed: {e}")

        if not url:
            return NodeOutput(error="no URL configured")

        try:
            check_url_safety(url, self._allowed_schemes, self._allow_private)
        except SSRFBlocked as e:
            return NodeOutput(error=f"blocked: {e}", metadata={"ssrf_blocked": True})

        body = None
        if self._method in ("POST", "PUT", "PATCH") and inputs:
            payload = next(iter(inputs.values())) if len(inputs) == 1 else inputs
            try:
                body = json.dumps(payload).encode()
            except (TypeError, ValueError) as e:
                return NodeOutput(error=f"cannot serialize request body: {e}")

        request = urllib.request.Request(url, data=body, method=self._method)
        for key, value in self._headers.items():
            request.add_header(key, value)
        if body is not None and "content-type" not in {k.lower() for k in self._headers}:
            request.add_header("Content-Type", "application/json")

        opener = urllib.request.build_opener(_LimitedRedirectHandler(
            self._allowed_schemes, self._allow_private,
        ))

        try:
            with opener.open(request, timeout=self._timeout) as response:
                raw = response.read(self._max_bytes + 1)
                if len(raw) > self._max_bytes:
                    return NodeOutput(error=f"response exceeded {self._max_bytes} bytes")
                status = response.status
                resp_headers = dict(response.headers)
        except urllib.error.HTTPError as e:
            return NodeOutput(error=f"HTTP {e.code}: {e.reason}", metadata={"status": e.code})
        except urllib.error.URLError as e:
            return NodeOutput(error=f"request failed: {e.reason}")
        except Exception as e:
            return NodeOutput(error=f"request error: {e}")

        text = raw.decode("utf-8", errors="replace")
        data: Any = text
        if self._parse_json:
            try:
                data = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                pass

        return NodeOutput(
            data=data,
            metadata={"status": status, "bytes": len(raw), "content_type": resp_headers.get("Content-Type", "")},
        )


class _LimitedRedirectHandler(urllib.request.HTTPRedirectHandler):
    max_redirections = MAX_REDIRECTS

    def __init__(self, allowed_schemes: set[str], allow_private: bool):
        self._allowed_schemes = allowed_schemes
        self._allow_private = allow_private

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        check_url_safety(newurl, self._allowed_schemes, self._allow_private)
        return super().redirect_request(req, fp, code, msg, headers, newurl)
