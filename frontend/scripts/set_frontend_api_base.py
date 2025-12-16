#!/usr/bin/env python3
"""Simple helper to inject the runtime API base into the static frontend."""

import argparse
from html import parser
import os
import re
from pathlib import Path


def update_api_base(index_path: Path, api_base: str) -> bool:
    pattern = re.compile(r'(<meta\s+name="api-base"\s+content=")([^"]*)(")', re.IGNORECASE)
    html = index_path.read_text(encoding="utf-8")
    updated_html, count = pattern.subn(rf'\1{api_base}\3', html, count=1)
    if count == 0:
        return False
    index_path.write_text(updated_html, encoding="utf-8")
    return True


def main():
    parser = argparse.ArgumentParser(description="Inject API base URL into the static frontend.")
    parser.add_argument("--api-base", dest="api_base", default=os.environ.get("API_BASE_URL"), help="API base URL (e.g. https://service.onrender.com/api)")
    parser.add_argument("--index", dest="index_path", default=None, help="Optional single HTML file to patch (defaults to patching every *.html under --frontend-dir).")
    parser.add_argument("--frontend-dir", dest="frontend_dir", default=".", ...)
    args = parser.parse_args()

    if not args.api_base:
        raise SystemExit("API base URL not provided. Pass --api-base or set API_BASE_URL.")

    api_base = args.api_base.rstrip("/")

    paths = []
    if args.index_path:
        index_path = Path(args.index_path)
        if not index_path.exists():
            raise SystemExit(f"Cannot find index file at {index_path}")
        paths = [index_path]
    else:
        frontend_dir = Path(args.frontend_dir)
        if not frontend_dir.exists() or not frontend_dir.is_dir():
            raise SystemExit(f"Cannot find frontend directory at {frontend_dir}")
        paths = sorted(frontend_dir.glob("*.html"))
        if not paths:
            raise SystemExit(f"No HTML files found under {frontend_dir}")

    updated = 0
    for path in paths:
        if update_api_base(path, api_base):
            updated += 1

    if updated == 0:
        raise SystemExit("Failed to locate <meta name=\"api-base\"> tag in the provided HTML file(s).")

    if len(paths) == 1:
        print(f"Updated API base to {api_base}")
    else:
        print(f"Updated API base to {api_base} in {updated} file(s)")


if __name__ == "__main__":
    main()
