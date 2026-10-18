"""Read Global X's full holdings dialog from its server-rendered page data."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from lxml import html


def _objects(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _objects(child)


def read_full_holdings(page: str, ticker: str) -> tuple[list[dict], str]:
    """Parse JSON only, never execute page JavaScript or substitute today's date."""
    root = html.fromstring(page)
    chunks = []
    decoder = json.JSONDecoder()
    prefix = "self.__next_f.push("
    for script in root.xpath("//script/text()"):
        script = script.strip()
        if not script.startswith(prefix):
            continue
        try:
            packet, _ = decoder.raw_decode(script[len(prefix):])
        except ValueError:
            continue
        if (isinstance(packet, list) and len(packet) == 2
                and packet[0] == 1 and isinstance(packet[1], str)):
            chunks.append(packet[1])

    matches = []
    # React Flight records may span multiple script chunks. Decode only JSON
    # records; imports, references and other Flight tags are not executable here.
    for line in "".join(chunks).splitlines():
        _, separator, payload = line.partition(":")
        if not separator or not payload.startswith(("[", "{")):
            continue
        try:
            record = json.loads(payload)
        except ValueError:
            continue
        for obj in _objects(record):
            if "holdings" in obj and obj.get("ticker") == ticker.upper():
                matches.append(obj)

    if len(matches) != 1:
        raise ValueError(f"{ticker}: expected one full Global X holdings dialog; found {len(matches)}")
    props = matches[0]
    date_text = props.get("asOfDate")
    if not isinstance(date_text, str):
        raise ValueError(f"{ticker}: Global X holdings as-of date missing")
    try:
        as_of = datetime.strptime(date_text, "%b %d, %Y").date()
    except ValueError as exc:
        raise ValueError(f"{ticker}: invalid Global X holdings as-of date {date_text!r}") from exc
    if as_of > datetime.now(timezone.utc).date():
        raise ValueError(f"{ticker}: future Global X holdings date {as_of}")
    rows = props["holdings"]
    required = {"ticker", "name", "netAssets", "shares", "marketValue", "sedol"}
    if (not isinstance(rows, list) or not rows
            or any(not isinstance(row, dict) or not required.issubset(row) for row in rows)):
        raise ValueError(f"{ticker}: incomplete Global X full holdings data")
    return rows, as_of.isoformat()
