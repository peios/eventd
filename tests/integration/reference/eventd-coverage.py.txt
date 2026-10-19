#!/usr/bin/env python3
"""The eventd TRM's anchors against the eventd suite's citations.

    python3 tests/tools/eventd-coverage.py      (from the repository root)

Prints each anchor no test cites (MISSING) and each citation of an anchor
the book no longer has (STALE), then a one-line total, and exits 1 if
either list is non-empty. A citation is an `eventd *<anchor>` token
anywhere in tests/eventd/*.lua: spec strings, concatenated or not, and
covered_by notes. The book is read from the learn checkout beside this
repository.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
BOOK = (ROOT.parent / "learn/peios.product/3--advanced-peios.antho"
        / "300--trms.shelf/400--eventd.book")
TESTS = ROOT / "tests/eventd"

anchors = {}
for md in sorted(BOOK.rglob("*.md")):
    for a in re.findall(r"\[\*([a-z0-9][a-z0-9.\-]*)\]", md.read_text()):
        anchors.setdefault(a, md.relative_to(BOOK))

cited = {}
for lua in sorted(TESTS.glob("*.lua")):
    for a in re.findall(r"eventd \*([a-z0-9][a-z0-9.\-]*[a-z0-9])", lua.read_text()):
        cited.setdefault(a, lua.name)

missing = sorted(set(anchors) - set(cited))
stale = sorted(set(cited) - set(anchors))
for a in missing:
    print(f"MISSING {a}  ({anchors[a]})")
for a in stale:
    print(f"STALE   {a}  ({cited[a]})")
print(f"TOTAL anchors={len(anchors)} cited={len(set(anchors) & set(cited))} "
      f"missing={len(missing)} stale={len(stale)}")
sys.exit(1 if missing or stale else 0)
