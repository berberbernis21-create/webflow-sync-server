#!/usr/bin/env python3
"""
DEPRECATED for production fixes — use scripts/resync_brooches.py instead.

Direct Webflow API moves skip the sync server pipeline (Shopify metafields/tags,
Google Merchant furniture cleanup, Render cache, duplicate-placement email).

    python scripts/resync_brooches.py

That calls POST /sync-by-ids on webflow-sync-server with forceReclassify.

This file remains only for emergency Webflow-only edits.
"""
from __future__ import annotations

import sys

print(
    "Use scripts/resync_brooches.py (sync server) so Shopify, Google, cache, and email run.\n"
    "  python scripts/resync_brooches.py",
    file=sys.stderr,
)
sys.exit(1)
