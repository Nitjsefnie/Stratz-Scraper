"""Configuration helpers for the web package."""

from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
TEMPLATE_DIR = BASE_DIR / "templates"

__all__ = ["BASE_DIR", "STATIC_DIR", "TEMPLATE_DIR"]
