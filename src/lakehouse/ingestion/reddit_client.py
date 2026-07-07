"""Reddit public JSON API ingestion (no OAuth app required for public listings).

Writes NDJSON pages to the landing zone, partitioned by subreddit and ingest date.
Respects Reddit's unauthenticated rate limits with conservative pacing.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from lakehouse.common.config import load_config
from lakehouse.common.logging import get_logger

log = get_logger(__name__)

_LISTING_URL = "https://www.reddit.com/r/{sub}/{listing}.json"
_SLEEP_SECONDS = 2.0


def _fetch_pages(sub: str, listing: str, pages: int, size: int, ua: str) -> Iterator[list[dict]]:
    after: str | None = None
    session = requests.Session()
    session.headers.update({"User-Agent": ua})
    for _ in range(pages):
        params: dict[str, Any] = {"limit": size}
        if after:
            params["after"] = after
        resp = session.get(_LISTING_URL.format(sub=sub, listing=listing), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()["data"]
        children = [child["data"] for child in data.get("children", [])]
        if not children:
            return
        yield children
        after = data.get("after")
        if after is None:
            return
        time.sleep(_SLEEP_SECONDS)


def ingest_reddit() -> Path:
    cfg = load_config()
    src = cfg.sources.reddit
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_root = Path(cfg.paths.landing) / "reddit" / f"ingest_date={ingest_date}"

    for sub in src.subreddits:
        out_dir = out_root / f"subreddit={sub}"
        out_dir.mkdir(parents=True, exist_ok=True)
        total = 0
        for page_idx, posts in enumerate(
            _fetch_pages(sub, src.listing, src.pages_per_subreddit, src.page_size, src.user_agent)
        ):
            path = out_dir / f"page_{page_idx:03d}.ndjson"
            with open(path, "w", encoding="utf-8") as fh:
                for post in posts:
                    fh.write(json.dumps(post, ensure_ascii=False) + "\n")
            total += len(posts)
        log.info("r/%s: %d posts -> %s", sub, total, out_dir)
    return out_root


if __name__ == "__main__":
    ingest_reddit()
