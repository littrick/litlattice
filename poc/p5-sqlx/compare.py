#!/usr/bin/env python3
"""Compare Go (database/sql + pgx) and Rust (sqlx) P5 fixtures semantically.

Usage: compare.py <go.json> <rust.json>
Exit 0 when all values are semantically equal; representation differences
(timestamp format, timezone alias, numeric trailing zeros) are reported but
accepted.
"""
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal


def ts(value):
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_pg_array(text):
    if text is None:
        return None
    assert text.startswith("{") and text.endswith("}"), text
    items, buf, quoted = [], "", False
    for ch in text[1:-1]:
        if quoted:
            if ch == '"':
                quoted = False
            else:
                buf += ch
        elif ch == '"':
            quoted = True
        elif ch == ",":
            items.append(buf)
            buf = ""
        else:
            buf += ch
    items.append(buf)
    return [None if i == "NULL" else i for i in items]


def main(go_path, rust_path):
    go = json.load(open(go_path))
    rust = json.load(open(rust_path))
    problems = []
    notes = []

    if go["citext_raw"] != rust["citext_raw"]:
        problems.append(f"citext_raw: {go['citext_raw']} != {rust['citext_raw']}")
    if go["citext_cast"] != rust["citext_cast"]:
        problems.append("citext_cast mismatch")

    gu, ru = go["user"], rust["user"]
    for key in ("id", "username", "display_name", "site_admin", "deleted_at"):
        if gu[key] != ru[key]:
            problems.append(f"user.{key}: {gu[key]} != {ru[key]}")
    for key in ("created_at", "updated_at"):
        if ts(gu[key]) != ts(ru[key]):
            problems.append(f"user.{key}: {gu[key]} != {ru[key]}")
        if gu[key] != ru[key]:
            notes.append(f"user.{key} format: {gu[key]} vs {ru[key]}")

    for i, (g, r) in enumerate(zip(go["roundtrip"], rust["roundtrip"])):
        if ts(g["ts"]) != ts(r["ts"]):
            problems.append(f"roundtrip[{i}].ts: {g['ts']} != {r['ts']}")
        if g["js"] != r["js"]:
            problems.append(f"roundtrip[{i}].js mismatch")
        if parse_pg_array(g["arr_text"]) != r["arr"]:
            problems.append(f"roundtrip[{i}].arr: {g['arr_text']} != {r['arr']}")
        if g["bin_hex"] != r["bin_hex"]:
            problems.append(f"roundtrip[{i}].bin: {g['bin_hex']} != {r['bin_hex']}")
        gnum = None if g["num"] is None else Decimal(g["num"])
        rnum = None if r["num"] is None else Decimal(r["num"])
        if gnum != rnum:
            problems.append(f"roundtrip[{i}].num: {g['num']} != {r['num']}")
        if g["num"] != r["num"]:
            notes.append(f"roundtrip[{i}].num representation: {g['num']} vs {r['num']}")
        if g["big"] != r["big"]:
            problems.append(f"roundtrip[{i}].big: {g['big']} != {r['big']}")
        if g["txt"] != r["txt"]:
            problems.append(f"roundtrip[{i}].txt: {g['txt']} != {r['txt']}")

    if go["commenter_select"] != rust["commenter_select"]:
        problems.append("commenter_select mismatch")

    gtz = go["session"]["timezone"]
    rtz = rust["session"]["timezone"]
    if gtz != rtz:
        notes.append(f"session timezone: Go={gtz} Rust={rtz} (sqlx forces TimeZone=UTC)")
    if go["session"]["datestyle"] != rust["session"]["datestyle"]:
        problems.append("datestyle mismatch")

    for note in notes:
        print(f"NOTE  {note}")
    for problem in problems:
        print(f"FAIL  {problem}")
    if problems:
        print("P5 SEMANTIC MISMATCH")
        return 1
    print("P5 SEMANTIC MATCH (representation differences listed above)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
