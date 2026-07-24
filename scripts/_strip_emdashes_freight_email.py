from pathlib import Path
import re

p = Path("lib/freightQuoteEmail.js")
t = p.read_text(encoding="utf-8")
before = t.count("\u2014") + t.count("\u2013")
print("before", before)

# En dash -> hyphen (ranges)
t = t.replace("\u2013", "-")
# Em dash as separator
t = t.replace(" \u2014 ", " | ")
t = t.replace("\u2014", " | ")
t = re.sub(r" +\| +", " | ", t)

# Empty placeholders accidentally turned into " | "
t = t.replace('return " | ";', 'return "-";')
t = t.replace('?? " | "', '?? "-"')
t = t.replace('|| " | "', '|| "-"')
t = t.replace('.trim() || " | "', '.trim() || "-"')

after = t.count("\u2014") + t.count("\u2013")
print("after", after)
for i, line in enumerate(t.splitlines(), 1):
    if "\u2014" in line or "\u2013" in line:
        print(i, line[:140])
p.write_text(t, encoding="utf-8")
print("wrote", p)
