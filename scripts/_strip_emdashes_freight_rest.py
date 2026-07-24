from pathlib import Path
import re

paths = [
    "lib/freightLocalEstimate.js",
    "lib/freightNationwideRate.js",
    "lib/freightPalletize.js",
    "lib/freightQuoteSecurity.js",
    "lib/freightQuoteValidation.js",
    "routes/freightQuote.js",
]

for rel in paths:
    p = Path(rel)
    t = p.read_text(encoding="utf-8")
    before = t.count("\u2014") + t.count("\u2013")
    t = t.replace("\u2013", "-")
    t = t.replace(" \u2014 ", " | ")
    t = t.replace("\u2014", " | ")
    t = re.sub(r" +\| +", " | ", t)
    after = t.count("\u2014") + t.count("\u2013")
    p.write_text(t, encoding="utf-8")
    print(f"{rel}: {before} -> {after}")
