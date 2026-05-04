#!/usr/bin/env python3
"""
Append \" (No Longer Available)\" to Luxury CMS item names (matches webflow-sync-server).

Reads an Excel export with columns: Name, Category, Webflow Item ID.
By default only rows with Category == \"Recently Sold\" are updated.

Requires env:
  WEBFLOW_TOKEN
  WEBFLOW_COLLECTION_ID  (24-char hex luxury CMS collection id)

Usage:
  python scripts/backfill_luxury_sold_names.py
  python scripts/backfill_luxury_sold_names.py --dry-run
  python scripts/backfill_luxury_sold_names.py --xlsx \"C:\\path\\file.xlsx\"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

try:
    import pandas as pd
except ImportError:
    print("Install: pip install pandas openpyxl", file=sys.stderr)
    raise SystemExit(1)

# Must match webflow-sync-server server.js
NO_LONGER_AVAILABLE_SUFFIX = " (No Longer Available)"
MARKER = "(No Longer Available)"
API = "https://api.webflow.com/v2/collections"
DEFAULT_XLSX = r"c:\Users\bberb\Downloads\webflow_product_names_categories_ONLY.xlsx"
DELAY_S = 1.1
MAX_RETRIES = 4


def append_suffix(title: str | None) -> str | None:
    if title is None:
        return None
    s = str(title).strip()
    if not s:
        return None
    if MARKER in s:
        return s
    return s + NO_LONGER_AVAILABLE_SUFFIX


def http_json(method: str, url: str, token: str, body: dict | None = None) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(err_body) if err_body.strip() else {}
        except json.JSONDecodeError:
            parsed = err_body
        return e.code, parsed


def patch_item_name(collection_id: str, item_id: str, new_name: str, token: str) -> tuple[bool, str]:
    url = f"{API}/{collection_id}/items/{item_id}"
    body = {"fieldData": {"name": new_name}}
    last_err = ""
    for attempt in range(MAX_RETRIES):
        status, resp = http_json("PATCH", url, token, body)
        if status in (200, 204):
            return True, ""
        if status == 429 and attempt < MAX_RETRIES - 1:
            wait = DELAY_S * (2**attempt) + 2
            time.sleep(wait)
            last_err = json.dumps(resp)
            continue
        last_err = json.dumps(resp) if isinstance(resp, dict) else str(resp)
        break
    return False, last_err


def get_item(collection_id: str, item_id: str, token: str) -> tuple[bool, dict | str]:
    url = f"{API}/{collection_id}/items/{item_id}"
    status, resp = http_json("GET", url, token, None)
    if status == 200 and isinstance(resp, dict):
        return True, resp
    return False, json.dumps(resp) if isinstance(resp, dict) else str(resp)


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill luxury sold listing names in Webflow CMS.")
    ap.add_argument("--xlsx", default=DEFAULT_XLSX, help="Path to Excel export")
    ap.add_argument("--dry-run", action="store_true", help="Parse Excel only; no API calls")
    ap.add_argument(
        "--all-rows",
        action="store_true",
        help="Ignore Category filter; attempt every row (use with care)",
    )
    args = ap.parse_args()

    path = os.path.expanduser(args.xlsx)
    if not os.path.isfile(path):
        print(f"File not found: {path}", file=sys.stderr)
        return 1

    df = pd.read_excel(path)
    for col in ("Name", "Category", "Webflow Item ID"):
        if col not in df.columns:
            print(f"Missing column {col!r}. Found: {list(df.columns)}", file=sys.stderr)
            return 1

    if not args.all_rows:
        mask = df["Category"].astype(str).str.strip() == "Recently Sold"
        df = df.loc[mask].copy()
    print(f"Rows to process: {len(df)}")

    token = (os.environ.get("WEBFLOW_TOKEN") or "").strip()
    collection_id = (os.environ.get("WEBFLOW_COLLECTION_ID") or "").strip()
    if args.dry_run:
        print("Dry run — no API calls.")
        for _, row in df.head(5).iterrows():
            name = str(row["Name"])
            iid = str(row["Webflow Item ID"]).strip()
            new_n = append_suffix(name) or name
            print(f"  {iid[:8]}…  {name[:50]}… -> …{new_n[-40:]}")
        if len(df) > 5:
            print(f"  … and {len(df) - 5} more")
        return 0

    if not token or not collection_id:
        print(
            "Set WEBFLOW_TOKEN and WEBFLOW_COLLECTION_ID in the environment, then re-run.\n"
            "Example (PowerShell):\n"
            '  $env:WEBFLOW_TOKEN = "…"\n'
            '  $env:WEBFLOW_COLLECTION_ID = "…"  # full 24-char collection id from Webflow',
            file=sys.stderr,
        )
        return 1

    if len(collection_id) != 24 or not all(c in "0123456789abcdefABCDEF" for c in collection_id):
        print(
            f"WEBFLOW_COLLECTION_ID should be a 24-character hex id (got length {len(collection_id)}).",
            file=sys.stderr,
        )
        return 1

    ok = 0
    skip = 0
    err = 0
    seen: set[str] = set()
    for idx, row in df.iterrows():
        item_id = str(row["Webflow Item ID"]).strip()
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        if len(item_id) != 24 or not all(c in "0123456789abcdefABCDEF" for c in item_id):
            print(f"Skip bad id at row {idx}: {item_id!r}", file=sys.stderr)
            err += 1
            continue

        good, live = get_item(collection_id, item_id, token)
        if not good or not isinstance(live, dict):
            print(f"GET failed {item_id}: {live}", file=sys.stderr)
            err += 1
            time.sleep(DELAY_S)
            continue

        current = (live.get("fieldData") or {}).get("name")
        if current is not None and MARKER in str(current):
            skip += 1
            time.sleep(DELAY_S)
            continue

        new_name = append_suffix(current) or append_suffix(row.get("Name"))
        if not new_name:
            print(f"Empty name {item_id}", file=sys.stderr)
            err += 1
            time.sleep(DELAY_S)
            continue

        success, msg = patch_item_name(collection_id, item_id, new_name, token)
        if success:
            ok += 1
            print(f"OK {ok}/{len(seen)} {item_id}")
        else:
            err += 1
            print(f"PATCH failed {item_id}: {msg}", file=sys.stderr)
        time.sleep(DELAY_S)

    print(f"Done. updated={ok} skipped_already_suffix={skip} errors={err} unique_ids={len(seen)}")
    return 0 if err == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
