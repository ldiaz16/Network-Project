#!/usr/bin/env python3
"""Simple helper to inject the runtime API base into the static frontend."""

import argparse
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
    parser.add_argument("--index", dest="index_path", default="frontend/index.html", help="Path to the frontend index file.")
    args = parser.parse_args()

    if not args.api_base:
        raise SystemExit("API base URL not provided. Pass --api-base or set API_BASE_URL.")

    index_path = Path(args.index_path)
    if not index_path.exists():
        raise SystemExit(f"Cannot find index file at {index_path}")

    if not update_api_base(index_path, args.api_base.rstrip("/")):
        raise SystemExit("Failed to locate <meta name=\"api-base\"> tag in the index file.")

    print(f"Updated API base to {args.api_base.rstrip('/')}")


if __name__ == "__main__":
    main()
