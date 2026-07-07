"""Kaggle Datasets API client (direct REST, no CLI dependency).

Auth: KAGGLE_USERNAME / KAGGLE_KEY (basic auth), same credentials as kaggle.json.
Downloads are idempotent: a completed dataset is skipped via a .done marker.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import requests

from lakehouse.common.config import load_config
from lakehouse.common.logging import get_logger
from lakehouse.common.secrets import resolve_secret

log = get_logger(__name__)

_API = "https://www.kaggle.com/api/v1/datasets/download/{ref}"
_CHUNK = 1 << 20  # 1 MiB


def download_dataset(ref: str, dest: Path, force: bool = False) -> Path:
    """Download and extract a Kaggle dataset (``owner/slug``) into *dest*."""
    dest.mkdir(parents=True, exist_ok=True)
    marker = dest / ".done"
    if marker.exists() and not force:
        log.info("Kaggle dataset %s already present at %s (skipping).", ref, dest)
        return dest

    auth = (resolve_secret("KAGGLE_USERNAME"), resolve_secret("KAGGLE_KEY"))
    archive = dest / "dataset.zip"
    log.info("Downloading Kaggle dataset %s ...", ref)
    with requests.get(_API.format(ref=ref), auth=auth, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with open(archive, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=_CHUNK):
                fh.write(chunk)

    with zipfile.ZipFile(archive) as zf:
        zf.extractall(dest)
    archive.unlink(missing_ok=True)
    marker.write_text(ref, encoding="utf-8")
    log.info("Extracted %s -> %s", ref, dest)
    return dest


def ingest_kaggle() -> Path:
    cfg = load_config()
    dest = Path(cfg.paths.landing) / "kaggle" / cfg.sources.kaggle.dataset.replace("/", "__")
    return download_dataset(cfg.sources.kaggle.dataset, dest)


if __name__ == "__main__":
    ingest_kaggle()
