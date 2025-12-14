"""
Lightweight async load tester for the API.

Examples:
python scripts/load_test_runner.py --base-url http://localhost:8000 --concurrency 10 --requests 100
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from typing import List

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fire-and-forget load test against the API.")
    parser.add_argument("--base-url", default="http://localhost:8000", help="API base URL (without trailing slash).")
    parser.add_argument("--requests", type=int, default=50, help="Total requests to send.")
    parser.add_argument("--concurrency", type=int, default=5, help="Number of concurrent workers.")
    parser.add_argument("--endpoint", default="/api/airlines?query=united", help="Endpoint to hit (relative to base).")
    return parser.parse_args()


async def hammer(client: httpx.AsyncClient, endpoint: str, results: List[float]) -> None:
    start = time.perf_counter()
    resp = await client.get(endpoint)
    latency_ms = (time.perf_counter() - start) * 1000
    results.append(latency_ms)
    resp.raise_for_status()


async def run_load_test(base_url: str, endpoint: str, total: int, concurrency: int) -> None:
    results: List[float] = []
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        semaphore = asyncio.Semaphore(concurrency)

        async def bounded_hammer():
            async with semaphore:
                await hammer(client, endpoint, results)

        tasks = [asyncio.create_task(bounded_hammer()) for _ in range(total)]
        await asyncio.gather(*tasks)

    if not results:
        print("No results collected.")
        return

    print(f"Completed {len(results)} requests to {base_url}{endpoint}")
    print(f"p50: {statistics.median(results):.2f} ms")
    print(f"p90: {statistics.quantiles(results, n=10)[8]:.2f} ms")
    print(f"max: {max(results):.2f} ms")


def main() -> None:
    args = parse_args()
    asyncio.run(run_load_test(args.base_url.rstrip("/"), args.endpoint, args.requests, args.concurrency))


if __name__ == "__main__":
    main()
