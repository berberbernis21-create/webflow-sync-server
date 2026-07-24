"""Scan live lostandfoundresale.com product pages for SEO keyword-soup titles."""
from __future__ import annotations

import csv
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import Request, urlopen

SITE = "https://www.lostandfoundresale.com"
UA = {"User-Agent": "LF-title-audit/1.0"}
OUT = Path(__file__).resolve().parent / "wonky_webflow_titles.csv"


def score(title: str) -> tuple[int, list[str]]:
    t = (title or "").strip()
    if not t:
        return 0, []
    reasons: list[str] = []
    s = 0
    commas = t.count(",")
    parts = [p.strip() for p in t.split(",") if p.strip()]
    kebab = sum(
        1
        for p in parts
        if "-" in p and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9\-]*", p)
    )
    if commas >= 3 and kebab >= 3:
        s += 50
        reasons.append(f"keyword-soup commas={commas} kebabs={kebab}")
    if commas >= 5:
        s += 20
        reasons.append(f"many-commas={commas}")
    if t.endswith("...") and commas >= 2:
        s += 25
        reasons.append("truncated-ellipsis")
    letters = re.sub(r"[^A-Za-z]", "", t)
    if (
        letters
        and sum(c.islower() for c in letters) / len(letters) > 0.9
        and commas >= 2
        and len(t) > 50
    ):
        s += 15
        reasons.append("mostly-lowercase-long")
    if len(parts) >= 6 and all(len(p) < 45 for p in parts[:10]):
        s += 20
        reasons.append(f"tag-list parts={len(parts)}")
    return s, reasons


def fetch_title(url: str) -> tuple[str, str, int, list[str]]:
    try:
        html = (
            urlopen(Request(url, headers=UA), timeout=25)
            .read(160000)
            .decode("utf-8", "replace")
        )
    except Exception as e:  # noqa: BLE001
        return url, "", 0, [f"fetch-error:{e}"]

    m = re.search(
        r'<h1[^>]*class="[^"]*title-heading[^"]*"[^>]*>(.*?)</h1>',
        html,
        re.I | re.S,
    )
    if not m:
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S)
    title = re.sub(r"<[^>]+>", "", m.group(1) if m else "")
    title = re.sub(r"\s+", " ", title).strip()
    if not title:
        tm = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
        title = re.sub(r"\s+", " ", tm.group(1) if tm else "").strip()
        title = re.sub(r"\s*\$\s*[\d,]+\.?\d*\s*$", "", title).strip()
    sc, reasons = score(title)
    return url, title, sc, reasons


def main() -> None:
    sitemap = (
        urlopen(Request(f"{SITE}/sitemap.xml", headers=UA), timeout=60)
        .read()
        .decode("utf-8", "replace")
    )
    urls = sorted(
        set(
            re.findall(
                rf"<loc>({re.escape(SITE)}/product/[^<]+)</loc>",
                sitemap,
            )
        )
    )
    print(f"product urls {len(urls)}")

    wonky: list[tuple[int, str, str, str]] = []
    errors = 0
    done = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = [ex.submit(fetch_title, u) for u in urls]
        for fut in as_completed(futs):
            url, title, sc, reasons = fut.result()
            done += 1
            if done % 100 == 0 or done == len(urls):
                print(
                    f"progress {done}/{len(urls)} wonky={len(wonky)} "
                    f"errors={errors} {time.time() - t0:.0f}s"
                )
            if any(r.startswith("fetch-error") for r in reasons):
                errors += 1
                continue
            if sc >= 40:
                wonky.append((sc, url, title, ";".join(reasons)))

    wonky.sort(reverse=True)
    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["score", "url", "title", "reasons"])
        w.writerows(wonky)

    print(f"DONE wonky={len(wonky)} errors={errors}")
    print(f"wrote {OUT}")
    for sc, url, title, reasons in wonky[:50]:
        print(f"{sc:3d} | {title[:110]}")
        print(f"     {url}")


if __name__ == "__main__":
    main()
