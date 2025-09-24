"""Utilities for dealing with request metadata."""

from __future__ import annotations

from flask import Request, request

__all__ = ["is_local_request"]


def _is_loopback_address(address: str) -> bool:
    address = (address or "").strip()
    if not address:
        return False
    return address in {"127.0.0.1", "::1"} or address.startswith("127.")


def is_local_request(active_request: Request | None = None) -> bool:
    """Return ``True`` when the incoming request originated from localhost."""

    active_request = active_request or request
    remote_addr = getattr(active_request, "remote_addr", None)
    if _is_loopback_address(remote_addr or ""):
        return True

    access_route = getattr(active_request, "access_route", []) or []
    for addr in access_route:
        if _is_loopback_address(addr or ""):
            return True

    forwarded_for = (active_request.headers.get("X-Forwarded-For", "") if active_request else "").split(",")
    return any(_is_loopback_address(addr) for addr in forwarded_for)
