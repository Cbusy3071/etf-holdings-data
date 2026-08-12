#!/usr/bin/env python3
"""
Provider-first ETF holdings pipeline for the Eight Bays model.

Primary sources:
  VGT   Vanguard snapshot + live Yahoo prices to drift weights between snapshots
  ACWI  iShares daily holdings CSV
  XLF   State Street daily holdings XLSX
  XLI   State Street daily holdings XLSX
  XLC   State Street daily holdings XLSX
  PPH   VanEck holdings CSV/HTML
  MLPX  Global X holdings CSV
  GRID  First Trust holdings HTML
  SOXQ  Invesco holdings export (fails safely if the export endpoint changes)

Output:
  data/ETF_Holdings_Latest.csv

The output schema is intentionally identical to the existing Power Query feed:
  source_date,retrieved_at_utc,fund_ticker,provider,rank,
  holding_ticker,holding_name,weight,shares_held,market_value_usd

Design principles:
  * Issuer/provider data is always the first-line source.
  * If complete market values are available, weights are recomputed from market
    values instead of using rounded website percentages.
  * VGT uses Vanguard's exact shares snapshot and live Yahoo prices. Stock splits
    after the Vanguard snapshot are incorporated before current market values are
    calculated.
  * Security identity is resolved internally as:
        ISIN -> SEDOL -> unambiguous ticker -> normalized name
    and the same canonical ticker/name is used across ETFs.
  * A truncated or malformed live response is NEVER allowed to replace a valid
    prior snapshot.
  * If a live provider fails, the previous valid rows for that ETF are reused
    with their original source_date/retrieved_at_utc preserved.
  * The combined CSV is only replaced when every requested ETF has either a
    newly validated provider pull or valid last-known-good rows.

Dependencies:
  pip install requests pandas yfinance openpyxl lxml html5lib
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urljoin

import pandas as pd
import requests
from lxml import html as lxml_html
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DEFAULT_OUT_DIR = Path("data")
DEFAULT_COMBINED_NAME = "ETF_Holdings_Latest.csv"
DEFAULT_TIMEOUT_SECONDS = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

PROVIDER_LABELS = {
    "VGT": "Vanguard",
    "ACWI": "iShares",
    "XLF": "SPDR/State Street",
    "XLI": "SPDR/State Street",
    "XLC": "SPDR/State Street",
    "PPH": "VanEck",
    "MLPX": "Global X",
    "GRID": "First Trust",
    "SOXQ": "Invesco",
}

# Hard floors protect against top-10/top-20/top-100 pages replacing full funds.
MIN_HOLDINGS_BY_TICKER = {
    "VGT": 250,
    "ACWI": 1500,
    "XLF": 60,
    "XLI": 60,
    "XLC": 20,
    "PPH": 20,
    "MLPX": 20,
    "GRID": 80,
    "SOXQ": 25,
}

# A new provider pull must retain at least this fraction of the previous valid
# number of rows. 90% is deliberately stricter than the old 85% guard.
MIN_PREVIOUS_COUNT_RATIO = 0.90

# ETF weights can differ slightly from 100 because of cash/rounding/derivatives.
MIN_TOTAL_WEIGHT = 98.0
MAX_TOTAL_WEIGHT = 102.0

# For VGT, tiny unpriced tails may use Vanguard's snapshot market value.
# Anything larger makes the run fail instead of silently treating a holding as 0.
VGT_MAX_TOTAL_FALLBACK_WEIGHT_PCT = 0.05  # 5 bps

OUTPUT_COLS = [
    "source_date",
    "retrieved_at_utc",
    "fund_ticker",
    "provider",
    "rank",
    "holding_ticker",
    "holding_name",
    "weight",
    "shares_held",
    "market_value_usd",
]

STD_COLS = [
    "raw_ticker",
    "raw_name",
    "weight",
    "shares",
    "market_value",
    "isin",
    "sedol",
]

SSGA_URL = (
    "https://www.ssga.com/us/en/intermediary/etfs/library-content/"
    "products/fund-data/etfs/us/holdings-daily-us-en-{t}.xlsx"
)

ISHARES_ACWI_URLS = [
    # Current official iShares download linked from the ACWI fund page.
    "https://www.ishares.com/us/products/239600/ishares-msci-acwi-etf/latest-holdings.csv",
]

INVESCO_PAGE_URL = (
    "https://www.invesco.com/us/en/financial-products/etfs/"
    "invesco-phlx-semiconductor-etf.html"
)

# Legacy official export route is retained as a best-effort attempt after first
# visiting the product page to establish cookies. If Invesco changes it again,
# the run falls back to last-known-good rather than publishing a partial table.
INVESCO_URLS = [
    (
        "https://www.invesco.com/us/financial-products/etfs/holdings/main/"
        "holdings/0?audienceType=Investor&action=download&ticker={t}"
    ),
]

GLOBALX_PAGE_URL = "https://www.globalxetfs.com/funds/{t}"

FIRSTTRUST_URLS = [
    "https://www.ftportfolios.com/Retail/Etf/EtfHoldings.aspx?Ticker={t}",
]

VANECK_PAGE_URL = (
    "https://www.vaneck.com/us/en/investments/"
    "pharmaceutical-etf-pph/holdings/"
)

VANGUARD_URLS = [
    "https://investor.vanguard.com/investment-products/etfs/profile/api/"
    "{t}/portfolio-holding/stock",
]

# Canonical display-name overrides can be keyed by identity, ticker or normalized
# name. Leave empty unless a provider gives a particularly ugly label.
CANONICAL_NAME_OVERRIDES: dict[str, str] = {
    # "TK:MSFT": "Microsoft Corporation",
    # "ISIN:US5949181045": "Microsoft Corporation",
}

# ---------------------------------------------------------------------------
# TYPES / ERRORS
# ---------------------------------------------------------------------------

class HoldingsError(RuntimeError):
    pass


@dataclass
class FetchResult:
    frame: pd.DataFrame
    source_date: str
    precision_method: str


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def http_get(session: requests.Session, url: str, timeout: int) -> requests.Response:
    r = session.get(url, timeout=(10, timeout), allow_redirects=True)
    if r.status_code != 200:
        raise HoldingsError(f"HTTP {r.status_code}: {url}")
    if len(r.content) < 100:
        raise HoldingsError(f"Suspiciously small response ({len(r.content)} bytes): {url}")
    return r


def try_urls(
    session: requests.Session,
    urls: Iterable[str],
    timeout: int,
) -> tuple[str, requests.Response]:
    errors: list[str] = []
    for url in urls:
        try:
            return url, http_get(session, url, timeout)
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise HoldingsError("All candidate URLs failed: " + " | ".join(errors))


# ---------------------------------------------------------------------------
# GENERIC HELPERS
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_today() -> str:
    return utc_now().date().isoformat()


def to_float(x: Any) -> float | None:
    if x is None or x is pd.NA:
        return None
    if isinstance(x, float) and pd.isna(x):
        return None
    s = str(x).strip()
    if not s or s.upper() in {"-", "--", "N/A", "NA", "NONE", "NAN"}:
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = s.replace(",", "").replace("%", "").replace("$", "").strip("() ")
    # Keep only numeric/scientific-notation characters.
    s = re.sub(r"[^0-9eE+\-.]", "", s)
    if not s:
        return None
    try:
        value = float(s)
        return -value if negative else value
    except ValueError:
        return None


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = [(str(c), str(c).strip().lower()) for c in df.columns]
    for candidate in candidates:
        cand = candidate.lower()
        for original, low in cols:
            if cand in low:
                return original
    return None


def clean_text(value: Any) -> str:
    if value is None or value is pd.NA:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_date_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = clean_text(value)
    if not text:
        return None

    # yyyy-mm-dd
    m = re.search(r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except ValueError:
            pass

    # mm/dd/yyyy
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2))).isoformat()
        except ValueError:
            pass

    # Month-name variants.
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def extract_date_from_text(text: str) -> str | None:
    # Prefer dates close to "as of".
    patterns = [
        r"(?i)as\s+of[^0-9A-Za-z]{0,20}([A-Za-z]+\s+\d{1,2},?\s+20\d{2})",
        r"(?i)as\s+of[^0-9]{0,20}(\d{1,2}/\d{1,2}/20\d{2})",
        r"(?i)as\s+of[^0-9]{0,20}(20\d{2}-\d{1,2}-\d{1,2})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            parsed = normalize_date_value(m.group(1))
            if parsed:
                return parsed

    # Otherwise inspect the first few explicit dates in the document.
    candidates = re.findall(
        r"\b(?:20\d{2}-\d{1,2}-\d{1,2}|\d{1,2}/\d{1,2}/20\d{2}|"
        r"[A-Za-z]+\s+\d{1,2},?\s+20\d{2})\b",
        text[:10000],
    )
    for candidate in candidates:
        parsed = normalize_date_value(candidate)
        if parsed:
            return parsed
    return None


def recursive_find_date(payload: Any) -> str | None:
    dateish_keys = (
        "asofdate",
        "as_of_date",
        "asof",
        "effectiveDate",
        "effective_date",
        "portfolioDate",
        "portfolio_date",
        "date",
    )
    stack = [payload]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            # First inspect likely date keys.
            for key, value in item.items():
                if str(key).lower() in {k.lower() for k in dateish_keys}:
                    parsed = normalize_date_value(value)
                    if parsed:
                        return parsed
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return None


def sniff_csv(text: str, must_contain: list[str]) -> tuple[pd.DataFrame, int]:
    lines = text.splitlines()
    header_idx = None
    for i, line in enumerate(lines[:50]):
        low = line.lower()
        if all(tok.lower() in low for tok in must_contain):
            header_idx = i
            break
    if header_idx is None:
        raise HoldingsError(
            f"Could not locate CSV header containing {must_contain}. "
            f"First lines: {lines[:5]}"
        )
    df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])), dtype=str)
    df.columns = [clean_text(c) for c in df.columns]
    return df, header_idx


def standardise(
    df: pd.DataFrame,
    *,
    ticker: str | None,
    name: str | None,
    weight: str | None,
    shares: str | None = None,
    mval: str | None = None,
    isin: str | None = None,
    sedol: str | None = None,
) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    out["raw_ticker"] = (
        df[ticker].map(clean_text) if ticker and ticker in df.columns else ""
    )
    out["raw_name"] = (
        df[name].map(clean_text) if name and name in df.columns else ""
    )
    out["weight"] = (
        df[weight].map(to_float) if weight and weight in df.columns else pd.NA
    )
    out["shares"] = (
        df[shares].map(to_float) if shares and shares in df.columns else pd.NA
    )
    out["market_value"] = (
        df[mval].map(to_float) if mval and mval in df.columns else pd.NA
    )
    out["isin"] = (
        df[isin].map(clean_text) if isin and isin in df.columns else ""
    )
    out["sedol"] = (
        df[sedol].map(clean_text) if sedol and sedol in df.columns else ""
    )

    out = out[(out["raw_name"] != "") | (out["raw_ticker"] != "")].copy()
    out["raw_ticker"] = out["raw_ticker"].astype(str).str.strip()
    out["raw_name"] = out["raw_name"].astype(str).str.strip()
    out["isin"] = out["isin"].astype(str).str.strip().str.upper()
    out["sedol"] = out["sedol"].astype(str).str.strip().str.upper()
    return out.reset_index(drop=True)[STD_COLS]


def drop_nonsecurity_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["raw_ticker"] = out["raw_ticker"].map(clean_text)
    out["raw_name"] = out["raw_name"].map(clean_text)

    # The existing Excel model is security/ticker based, so blank-ticker cash,
    # FX and derivative rows are excluded from the holdings stream.
    out = out[out["raw_ticker"] != ""].copy()

    ticker_upper = out["raw_ticker"].str.upper()
    name_upper = out["raw_name"].str.upper()

    # Remove obvious repeated headers/disclaimers and provider cash/FX lines.
    bad = ticker_upper.isin(
        {
            "TICKER", "SYMBOL", "N/A", "NA", "NONE", "--",
            "USD", "EUR", "GBP", "CHF", "CAD", "AUD", "JPY", "HKD",
            "CNY", "TWD", "KRW",
        }
    )
    cash_fx = (
        ticker_upper.str.startswith("$")
        | ticker_upper.str.contains(r"CASH", regex=True)
        | name_upper.str.fullmatch(
            r"(US DOLLAR|EURO|POUND STERLING|SWISS FRANC|"
            r"HONG KONG DOLLAR|YUAN RENMINBI|BRAZILIAN REAL|"
            r"NEW TAIWAN DOLLAR|OTHER/CASH|CASH)",
            na=False,
        )
    )
    out = out[~(bad | cash_fx)].copy()
    return out.reset_index(drop=True)


def maybe_recompute_from_market_value(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """Use exact market values when coverage is complete.

    We only recompute when every published security row has a non-negative
    market value. This prevents a partially parsed value column from distorting
    the denominator.
    """
    out = df.copy()
    if out.empty:
        return out, "provider_weight"

    mval = pd.to_numeric(out["market_value"], errors="coerce")
    if mval.notna().all() and float(mval.sum()) > 0:
        total = float(mval.sum())
        out["weight"] = mval / total * 100.0
        return out, "market_value_recomputed"

    return out, "provider_weight"



def find_download_link(
    html_text: str,
    base_url: str,
    *,
    text_contains: tuple[str, ...] = (),
    href_contains: tuple[str, ...] = (),
) -> str | None:
    """Return the first matching absolute link from provider HTML."""
    try:
        root = lxml_html.fromstring(html_text)
    except Exception:
        return None

    text_tokens = tuple(t.lower() for t in text_contains)
    href_tokens = tuple(t.lower() for t in href_contains)

    ranked: list[tuple[int, str]] = []
    for a in root.xpath("//a[@href]"):
        href = clean_text(a.get("href"))
        if not href or href.lower().startswith(("javascript:", "mailto:", "#")):
            continue
        label = " ".join(clean_text(x) for x in a.itertext()).strip().lower()
        href_low = href.lower()

        text_ok = not text_tokens or all(t in label for t in text_tokens)
        href_ok = not href_tokens or all(t in href_low for t in href_tokens)
        if not (text_ok and href_ok):
            continue

        score = 0
        if "download" in label:
            score += 3
        if "full holdings" in label:
            score += 3
        if href_low.endswith((".csv", ".xls", ".xlsx")):
            score += 3
        if "holding" in href_low:
            score += 1
        ranked.append((score, urljoin(base_url, href)))

    if not ranked:
        return None
    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1]


def excel_with_detected_header(
    content: bytes,
    *,
    required_tokens: tuple[str, ...] = ("ticker",),
    max_scan_rows: int = 40,
) -> tuple[pd.DataFrame, str | None]:
    """Parse a provider XLS/XLSX whose metadata precedes the real header."""
    raw = pd.read_excel(io.BytesIO(content), header=None, dtype=str)

    hdr = None
    for i in range(min(max_scan_rows, len(raw))):
        joined = " | ".join(clean_text(v).lower() for v in raw.iloc[i].tolist())
        if all(tok.lower() in joined for tok in required_tokens):
            hdr = i
            break

    if hdr is None:
        raise HoldingsError(
            f"Could not find Excel header containing {required_tokens}"
        )

    source_date = None
    for i in range(hdr):
        row_text = " ".join(clean_text(v) for v in raw.iloc[i].tolist())
        parsed = extract_date_from_text(row_text)
        if parsed:
            source_date = parsed
            break

    df = raw.iloc[hdr + 1 :].copy()
    df.columns = [clean_text(c) for c in raw.iloc[hdr].tolist()]
    return df, source_date


ISHARES_EXCHANGE_SUFFIX = {
    "Taiwan Stock Exchange": ".TW",
    "Korea Exchange (Stock Market)": ".KS",
    "Tokyo Stock Exchange": ".T",
    "Hong Kong Exchanges And Clearing Ltd": ".HK",
    "London Stock Exchange": ".L",
    "Euronext Amsterdam": ".AS",
    "SIX Swiss Exchange": ".SW",
    "Xetra": ".DE",
    "Nyse Euronext - Euronext Paris": ".PA",
    "Toronto Stock Exchange": ".TO",
    "Asx - All Markets": ".AX",
    "Bolsa De Madrid": ".MC",
    "Borsa Italiana": ".MI",
    "Nasdaq Omx Helsinki Ltd.": ".HE",
    "Nasdaq Omx Stockholm": ".ST",
    "Nasdaq Omx Nordic": ".ST",
    "Nasdaq Omx Copenhagen": ".CO",
    "Oslo Bors Asa": ".OL",
    "Euronext Brussels": ".BR",
    "Euronext Lisbon": ".LS",
    "Singapore Exchange": ".SI",
    "New Zealand Exchange Ltd": ".NZ",
    "Tel Aviv Stock Exchange": ".TA",
}


def qualify_ishares_ticker(raw_ticker: Any, exchange: Any) -> str:
    ticker = clean_text(raw_ticker)
    exch = clean_text(exchange)
    if not ticker:
        return ""
    suffix = ISHARES_EXCHANGE_SUFFIX.get(exch)
    if not suffix:
        return ticker
    # Do not double-qualify a provider ticker.
    if "." in ticker:
        return ticker
    return ticker + suffix


def parse_firsttrust_dom(html_text: str) -> pd.DataFrame:
    """Parse First Trust's holdings rows even when pandas.read_html misses them."""
    try:
        root = lxml_html.fromstring(html_text)
    except Exception as exc:
        raise HoldingsError(f"First Trust HTML parse failed: {exc}") from exc

    rows: list[dict[str, Any]] = []
    for tr in root.xpath("//tr"):
        cells = [
            " ".join(clean_text(x) for x in cell.itertext()).strip()
            for cell in tr.xpath("./th|./td")
        ]
        cells = [c for c in cells if c != ""]
        if len(cells) < 6:
            continue

        # Expected layout:
        # Security Name | Identifier | CUSIP | Classification |
        # Shares / Quantity | Market Value | Weighting
        weight = to_float(cells[-1])
        mval = to_float(cells[-2])
        shares = to_float(cells[-3])
        if weight is None or mval is None or shares is None:
            continue

        ticker = clean_text(cells[1])
        name = clean_text(cells[0])
        if not ticker or ticker.lower() in {"identifier", "ticker"}:
            continue

        rows.append(
            {
                "raw_ticker": ticker,
                "raw_name": name,
                "weight": weight,
                "shares": shares,
                "market_value": mval,
                "isin": "",
                "sedol": "",
            }
        )

    if not rows:
        raise HoldingsError("no First Trust holdings rows found in DOM")

    return pd.DataFrame(rows, columns=STD_COLS)


def parse_downloaded_holdings(
    response: requests.Response,
    *,
    provider: str,
    source_date_hint: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    """Parse a provider download as CSV or Excel using flexible column names."""
    ctype = response.headers.get("content-type", "").lower()
    final_url = str(response.url).lower()

    if (
        "spreadsheet" in ctype
        or "excel" in ctype
        or final_url.endswith((".xls", ".xlsx"))
        or response.content[:2] == b"PK"
    ):
        df, source_date = excel_with_detected_header(
            response.content,
            required_tokens=("ticker",),
        )
        return df, source_date or source_date_hint

    text = response.text
    if "ticker" in text[:10000].lower() and "," in text[:2000]:
        df, _ = sniff_csv(text, ["ticker"])
        return df, extract_date_from_text(text[:20000]) or source_date_hint

    raise HoldingsError(
        f"{provider}: download was neither recognizable CSV nor Excel "
        f"(content-type={ctype}, final_url={response.url})"
    )


# ---------------------------------------------------------------------------
# PROVIDER FETCHERS
# ---------------------------------------------------------------------------

def fetch_ssga(
    session: requests.Session, etf: str, timeout: int
) -> FetchResult:
    url = SSGA_URL.format(t=etf.lower())
    r = http_get(session, url, timeout)

    raw = pd.read_excel(io.BytesIO(r.content), header=None, dtype=str)
    hdr = None
    for i in range(min(20, len(raw))):
        vals = " ".join(clean_text(v).lower() for v in raw.iloc[i].tolist())
        if "ticker" in vals and "weight" in vals:
            hdr = i
            break
    if hdr is None:
        raise HoldingsError(f"{etf}: SSGA holdings header not found")

    source_date = None
    for i in range(hdr):
        row_text = " ".join(clean_text(v) for v in raw.iloc[i].tolist())
        parsed = extract_date_from_text(row_text)
        if parsed:
            source_date = parsed
            break

    df = raw.iloc[hdr + 1 :].copy()
    df.columns = [clean_text(c) for c in raw.iloc[hdr].tolist()]
    result = standardise(
        df,
        ticker=pick_col(df, ["ticker"]),
        name=pick_col(df, ["name", "security"]),
        weight=pick_col(df, ["weight"]),
        shares=pick_col(df, ["shares held", "shares"]),
        mval=pick_col(df, ["market value"]),
        isin=pick_col(df, ["isin"]),
        sedol=pick_col(df, ["sedol"]),
    )
    result = drop_nonsecurity_rows(result)
    result, precision = maybe_recompute_from_market_value(result)
    return FetchResult(result, source_date or iso_today(), precision)


def fetch_ishares(
    session: requests.Session, etf: str, timeout: int
) -> FetchResult:
    _, r = try_urls(session, ISHARES_ACWI_URLS, timeout)
    text = r.text
    df, _ = sniff_csv(text, ["ticker", "name", "weight"])

    ticker_col = pick_col(df, ["ticker"])
    exchange_col = pick_col(df, ["exchange"])
    if ticker_col and exchange_col:
        df = df.copy()
        df[ticker_col] = [
            qualify_ishares_ticker(t, ex)
            for t, ex in zip(df[ticker_col], df[exchange_col])
        ]

    result_all = standardise(
        df,
        ticker=ticker_col,
        name=pick_col(df, ["name"]),
        weight=pick_col(df, ["weight"]),
        shares=pick_col(df, ["quantity", "shares"]),
        mval=pick_col(df, ["market value"]),
        isin=pick_col(df, ["isin"]),
        sedol=pick_col(df, ["sedol"]),
    )

    # Recompute before removing cash/FX so the denominator remains the provider's
    # full portfolio value rather than an equity-only renormalisation.
    result_all, precision = maybe_recompute_from_market_value(result_all)
    result = drop_nonsecurity_rows(result_all)

    return FetchResult(
        result,
        extract_date_from_text(text[:20000]) or iso_today(),
        precision,
    )


def fetch_invesco(
    session: requests.Session, etf: str, timeout: int
) -> FetchResult:
    """Best-effort official Invesco export.

    Invesco's portfolio table is dynamically loaded and its legacy CSV endpoint
    currently returns 406 on some hosts. We first visit the official SOXQ page
    to establish cookies, then retry the export with browser-like download
    headers. If Invesco still rejects it, the pipeline uses last-known-good.
    """
    page = http_get(session, INVESCO_PAGE_URL, timeout)
    source_date = extract_date_from_text(page.text[:50000]) or iso_today()

    # If Invesco exposes a real downloadable link in the page, prefer it.
    discovered = find_download_link(
        page.text,
        page.url,
        text_contains=("export",),
    )
    candidates = []
    if discovered:
        candidates.append(discovered)
    candidates.extend(u.format(t=etf) for u in INVESCO_URLS)

    errors: list[str] = []
    for url in dict.fromkeys(candidates):
        try:
            headers = {
                "Referer": INVESCO_PAGE_URL,
                "Accept": (
                    "text/csv,application/csv,application/vnd.ms-excel,"
                    "application/octet-stream;q=0.9,*/*;q=0.8"
                ),
            }
            r = session.get(
                url,
                headers=headers,
                timeout=(10, timeout),
                allow_redirects=True,
            )
            if r.status_code != 200 or len(r.content) < 100:
                raise HoldingsError(
                    f"HTTP {r.status_code}, {len(r.content)} bytes"
                )

            parsed, dl_date = parse_downloaded_holdings(
                r,
                provider="Invesco",
                source_date_hint=source_date,
            )
            result_all = standardise(
                parsed,
                ticker=pick_col(parsed, ["holding ticker", "ticker", "symbol"]),
                name=pick_col(parsed, ["name", "security", "holding"]),
                weight=pick_col(parsed, ["weight", "%"]),
                shares=pick_col(parsed, ["shares", "quantity"]),
                mval=pick_col(parsed, ["market value", "marketvalue"]),
                isin=pick_col(parsed, ["isin", "security identifier"]),
                sedol=pick_col(parsed, ["sedol"]),
            )
            result_all, precision = maybe_recompute_from_market_value(result_all)
            result = drop_nonsecurity_rows(result_all)
            return FetchResult(result, dl_date or source_date, precision)
        except Exception as exc:
            errors.append(f"{url}: {exc}")

    raise HoldingsError(" | ".join(errors))


def fetch_globalx(
    session: requests.Session, etf: str, timeout: int
) -> FetchResult:
    page_url = GLOBALX_PAGE_URL.format(t=etf.upper())
    page = http_get(session, page_url, timeout)
    source_date = extract_date_from_text(page.text[:50000]) or iso_today()

    csv_url = find_download_link(
        page.text,
        page.url,
        text_contains=("full holdings",),
        href_contains=(".csv",),
    )
    if not csv_url:
        # Some page versions have no visible text on the anchor; fall back to
        # any CSV link that contains the fund ticker or "holding".
        try:
            root = lxml_html.fromstring(page.text)
            links = []
            for a in root.xpath("//a[@href]"):
                href = clean_text(a.get("href"))
                low = href.lower()
                if ".csv" not in low:
                    continue
                score = 0
                if etf.lower() in low:
                    score += 3
                if "holding" in low:
                    score += 2
                links.append((score, urljoin(page.url, href)))
            if links:
                links.sort(key=lambda x: x[0], reverse=True)
                csv_url = links[0][1]
        except Exception:
            pass

    if not csv_url:
        raise HoldingsError(f"{etf}: Full Holdings CSV link not found")

    r = http_get(session, csv_url, timeout)
    parsed, dl_date = parse_downloaded_holdings(
        r,
        provider="Global X",
        source_date_hint=source_date,
    )

    result_all = standardise(
        parsed,
        ticker=pick_col(parsed, ["ticker"]),
        name=pick_col(parsed, ["name"]),
        weight=pick_col(parsed, ["% of net assets", "net assets", "weight"]),
        shares=pick_col(parsed, ["shares held", "shares", "quantity"]),
        mval=pick_col(parsed, ["market value"]),
        isin=pick_col(parsed, ["isin"]),
        sedol=pick_col(parsed, ["sedol"]),
    )
    result_all, precision = maybe_recompute_from_market_value(result_all)
    result = drop_nonsecurity_rows(result_all)

    return FetchResult(result, dl_date or source_date, precision)


def fetch_firsttrust(
    session: requests.Session, etf: str, timeout: int
) -> FetchResult:
    urls = [u.format(t=etf) for u in FIRSTTRUST_URLS]
    _, page = try_urls(session, urls, timeout)
    source_date = extract_date_from_text(page.text[:50000]) or iso_today()

    errors: list[str] = []

    # First Trust's holdings are present in the page DOM, but the table markup
    # is not reliably recognized by pandas.read_html. Parse row cells directly.
    try:
        result_all = parse_firsttrust_dom(page.text)
        result_all, precision = maybe_recompute_from_market_value(result_all)
        result = drop_nonsecurity_rows(result_all)
        if len(result) >= MIN_HOLDINGS_BY_TICKER[etf]:
            return FetchResult(result, source_date, precision)
        errors.append(f"DOM parser returned only {len(result)} rows")
    except Exception as exc:
        errors.append(f"DOM parser: {exc}")

    # Secondary path: follow any actual Excel/export link exposed in the page.
    export_url = (
        find_download_link(
            page.text,
            page.url,
            text_contains=("export", "excel"),
        )
        or find_download_link(
            page.text,
            page.url,
            href_contains=("excel",),
        )
    )
    if export_url:
        try:
            r = http_get(session, export_url, timeout)
            parsed, dl_date = parse_downloaded_holdings(
                r,
                provider="First Trust",
                source_date_hint=source_date,
            )
            result_all = standardise(
                parsed,
                ticker=pick_col(parsed, ["identifier", "ticker", "symbol"]),
                name=pick_col(parsed, ["security name", "security", "name"]),
                weight=pick_col(parsed, ["weighting", "weight", "%"]),
                shares=pick_col(parsed, ["shares", "quantity"]),
                mval=pick_col(parsed, ["market value"]),
                isin=pick_col(parsed, ["isin"]),
                sedol=pick_col(parsed, ["sedol"]),
            )
            result_all, precision = maybe_recompute_from_market_value(result_all)
            result = drop_nonsecurity_rows(result_all)
            return FetchResult(result, dl_date or source_date, precision)
        except Exception as exc:
            errors.append(f"export parser: {exc}")

    raise HoldingsError(f"{etf}: " + " | ".join(errors))


def fetch_vaneck(
    session: requests.Session, etf: str, timeout: int
) -> FetchResult:
    page = http_get(session, VANECK_PAGE_URL, timeout)
    source_date = extract_date_from_text(page.text[:50000]) or iso_today()
    errors: list[str] = []

    # VanEck's official page exposes a "Download XLS" control. Resolve that link
    # dynamically so future filename/date changes do not require code edits.
    xls_url = (
        find_download_link(
            page.text,
            page.url,
            text_contains=("download", "xls"),
        )
        or find_download_link(
            page.text,
            page.url,
            href_contains=(".xls",),
        )
    )
    if xls_url:
        try:
            r = http_get(session, xls_url, timeout)
            parsed, dl_date = parse_downloaded_holdings(
                r,
                provider="VanEck",
                source_date_hint=source_date,
            )
            result_all = standardise(
                parsed,
                ticker=pick_col(parsed, ["ticker", "symbol"]),
                name=pick_col(parsed, ["holding name", "name", "holding", "security"]),
                weight=pick_col(parsed, ["% of net assets", "% of net", "weight"]),
                shares=pick_col(parsed, ["shares", "quantity"]),
                mval=pick_col(parsed, ["market value"]),
                isin=pick_col(parsed, ["isin"]),
                sedol=pick_col(parsed, ["sedol"]),
            )
            result_all, precision = maybe_recompute_from_market_value(result_all)
            result = drop_nonsecurity_rows(result_all)
            return FetchResult(result, dl_date or source_date, precision)
        except Exception as exc:
            errors.append(f"XLS parser: {exc}")

    # HTML fallback for page versions where the link is generated client-side.
    try:
        tables = pd.read_html(io.StringIO(page.text))
        candidates = [
            t for t in tables
            if len(t) >= 20
            and any(
                "weight" in clean_text(c).lower()
                or "% of net" in clean_text(c).lower()
                for c in t.columns
            )
        ]
        if not candidates:
            raise HoldingsError("no holdings table")
        best = max(candidates, key=len).copy()
        best.columns = [clean_text(c) for c in best.columns]
        parsed = best.astype(str)

        result_all = standardise(
            parsed,
            ticker=pick_col(parsed, ["ticker", "symbol"]),
            name=pick_col(parsed, ["holding name", "name", "holding", "security"]),
            weight=pick_col(parsed, ["% of net assets", "% of net", "weight", "%"]),
            shares=pick_col(parsed, ["shares", "quantity"]),
            mval=pick_col(parsed, ["market value"]),
            isin=pick_col(parsed, ["isin"]),
            sedol=pick_col(parsed, ["sedol"]),
        )
        result_all, precision = maybe_recompute_from_market_value(result_all)
        result = drop_nonsecurity_rows(result_all)
        return FetchResult(result, source_date, precision)
    except Exception as exc:
        errors.append(f"HTML parser: {exc}")

    raise HoldingsError(f"{etf}: " + " | ".join(errors))


def _walk_dict_lists(value: Any):
    if isinstance(value, list):
        if value and all(isinstance(item, dict) for item in value):
            yield value
        for item in value:
            yield from _walk_dict_lists(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _walk_dict_lists(item)


def _score_vanguard_list(rows: list[dict[str, Any]]) -> int:
    score = 0
    for row in rows[:20]:
        keys = {str(k).lower() for k in row}
        if any(k in keys for k in {"ticker", "tickersymbol", "symbol"}):
            score += 3
        if any(k in keys for k in {"sharesheld", "shares", "quantity"}):
            score += 3
        if any(k in keys for k in {"marketvalue", "marketval"}):
            score += 2
        if any(k in keys for k in {"longname", "shortname", "name"}):
            score += 1
    return score


def fetch_vanguard(
    session: requests.Session, etf: str, timeout: int
) -> FetchResult:
    urls = [u.format(t=etf.lower()) for u in VANGUARD_URLS]
    _, r = try_urls(session, urls, timeout)

    try:
        payload = r.json()
    except json.JSONDecodeError as exc:
        raise HoldingsError(f"{etf}: Vanguard response was not JSON: {exc}") from exc

    candidates = list(_walk_dict_lists(payload))
    if not candidates:
        raise HoldingsError(f"{etf}: no holdings list found in Vanguard JSON")

    entities = max(candidates, key=_score_vanguard_list)
    if _score_vanguard_list(entities) <= 0:
        raise HoldingsError(f"{etf}: Vanguard JSON did not contain recognizable holdings")

    rows: list[dict[str, Any]] = []
    for e in entities:
        rows.append(
            {
                "raw_ticker": clean_text(
                    e.get("ticker")
                    or e.get("tickerSymbol")
                    or e.get("symbol")
                    or ""
                ),
                "raw_name": clean_text(
                    e.get("longName")
                    or e.get("shortName")
                    or e.get("name")
                    or e.get("securityName")
                    or ""
                ),
                "weight": to_float(
                    e.get("percentWeight")
                    or e.get("weight")
                    or e.get("percent")
                ),
                "shares": to_float(
                    e.get("sharesHeld")
                    or e.get("shares")
                    or e.get("quantity")
                ),
                "market_value": to_float(
                    e.get("marketValue")
                    or e.get("marketVal")
                    or e.get("value")
                ),
                "isin": clean_text(e.get("isin") or ""),
                "sedol": clean_text(e.get("sedol") or ""),
            }
        )

    result = pd.DataFrame(rows, columns=STD_COLS)
    result = drop_nonsecurity_rows(result)
    source_date = recursive_find_date(payload) or iso_today()

    # Do not recompute from Vanguard snapshot market values here. VGT is
    # intentionally drifted to current prices below.
    return FetchResult(result, source_date, "vanguard_snapshot")


# ---------------------------------------------------------------------------
# VGT DRIFTING
# ---------------------------------------------------------------------------

def yahoo_ticker(raw_ticker: str) -> str:
    """Convert common US provider tickers into Yahoo syntax.

    Preserve exchange suffixes such as 2330.TW. Convert class-share dots for US
    tickers such as BRK.B -> BRK-B.
    """
    s = clean_text(raw_ticker).upper()
    s = re.sub(r"\s+(US|UN|UW|UQ|UR|UF)$", "", s)

    # A dot followed by 1-4 letters at the end is probably an exchange suffix
    # (e.g. .TW, .KS). Keep it. Otherwise treat the dot as a US share-class
    # separator for Yahoo.
    if re.fullmatch(r"[A-Z0-9]+\.([A-Z]{1,4})", s):
        suffix = s.rsplit(".", 1)[1]
        if suffix in {"TW", "KS", "L", "PA", "AS", "DE", "SW", "TO", "HK", "T"}:
            return s
    return s.replace("/", "-").replace(".", "-")


def _field_frame(data: pd.DataFrame, field: str, tickers: list[str]) -> pd.DataFrame:
    """Extract one yfinance field into a ticker-column DataFrame."""
    if isinstance(data.columns, pd.MultiIndex):
        if field not in data.columns.get_level_values(0):
            return pd.DataFrame(index=data.index)
        frame = data[field]
        if isinstance(frame, pd.Series):
            frame = frame.to_frame(tickers[0] if tickers else field)
        return frame

    if field not in data.columns:
        return pd.DataFrame(index=data.index)

    series = data[field]
    if isinstance(series, pd.Series):
        name = tickers[0] if len(tickers) == 1 else field
        return series.to_frame(name)
    return series


def drift_vgt_with_yahoo(
    df: pd.DataFrame,
    snapshot_date: str,
) -> pd.DataFrame:
    """Recompute VGT weights from snapshot shares x current prices.

    Split handling:
      shares_today = snapshot_shares * product(stock_split_ratios_after_snapshot)

    Missing-price policy:
      * never assign a material security a value of zero;
      * tiny unpriced tails may retain their Vanguard snapshot market value;
      * if the total snapshot weight of fallbacks exceeds 5 bps, fail the live
        VGT pull and fall back to the prior validated CSV instead.
    """
    import yfinance as yf

    out = df.copy()
    if out.empty:
        raise HoldingsError("VGT: Vanguard returned no holdings")

    snapshot_dt = datetime.fromisoformat(snapshot_date).date()
    start = (snapshot_dt - timedelta(days=3)).isoformat()

    out["_yf"] = out["raw_ticker"].map(yahoo_ticker)
    tickers = sorted({t for t in out["_yf"] if t})
    if not tickers:
        raise HoldingsError("VGT: no Yahoo-priceable tickers")

    print(f"[VGT] Pricing {len(tickers)} securities via Yahoo...", file=sys.stderr)

    try:
        hist = yf.download(
            tickers=tickers,
            start=start,
            interval="1d",
            actions=True,
            auto_adjust=False,
            keepna=False,
            progress=False,
            threads=True,
            timeout=15,
            group_by="column",
        )
    except Exception as exc:
        raise HoldingsError(f"VGT: Yahoo bulk download failed: {exc}") from exc

    if hist is None or len(hist) == 0:
        raise HoldingsError("VGT: Yahoo returned no price history")

    closes = _field_frame(hist, "Close", tickers)
    splits = _field_frame(hist, "Stock Splits", tickers)

    latest_price: dict[str, float] = {}
    split_factor: dict[str, float] = {}

    for t in tickers:
        if t in closes.columns:
            series = pd.to_numeric(closes[t], errors="coerce").dropna()
            if not series.empty:
                latest_price[t] = float(series.iloc[-1])

        factor = 1.0
        if t in splits.columns:
            s = pd.to_numeric(splits[t], errors="coerce").fillna(0.0)
            # Split events on/after the snapshot date affect today's share count.
            for idx, value in s.items():
                idx_date = pd.Timestamp(idx).date()
                if idx_date > snapshot_dt and value not in (0, 1) and value > 0:
                    factor *= float(value)
        split_factor[t] = factor

    snapshot_mval = pd.to_numeric(out["market_value"], errors="coerce")
    snapshot_weight = pd.to_numeric(out["weight"], errors="coerce")

    # If provider weights are missing but market values are present, reconstruct
    # snapshot weights solely for missing-price significance checks.
    if snapshot_weight.notna().sum() < len(out) and snapshot_mval.notna().all():
        total_snap = float(snapshot_mval.sum())
        if total_snap > 0:
            snapshot_weight = snapshot_mval / total_snap * 100.0

    live_values: list[float] = []
    adjusted_shares: list[float | None] = []
    fallback_weight = 0.0
    fallback_names: list[str] = []

    for i, row in out.iterrows():
        t = row["_yf"]
        shares = to_float(row["shares"])
        px = latest_price.get(t)
        factor = split_factor.get(t, 1.0)

        if shares is not None and shares > 0 and px is not None and px > 0:
            today_shares = shares * factor
            live_values.append(today_shares * px)
            adjusted_shares.append(today_shares)
            continue

        mv = to_float(row["market_value"])
        w = (
            float(snapshot_weight.iloc[i])
            if i in snapshot_weight.index and pd.notna(snapshot_weight.iloc[i])
            else 0.0
        )
        fallback_weight += max(w, 0.0)
        fallback_names.append(f"{row['raw_ticker']}({w:.4f}%)")

        if mv is None or mv < 0:
            raise HoldingsError(
                f"VGT: no live price and no snapshot market value for "
                f"{row['raw_ticker']} {row['raw_name']}"
            )
        live_values.append(mv)
        adjusted_shares.append(shares)

    if fallback_weight > VGT_MAX_TOTAL_FALLBACK_WEIGHT_PCT:
        raise HoldingsError(
            "VGT: unpriced holdings are too material to drift safely: "
            f"{fallback_weight:.4f}% > "
            f"{VGT_MAX_TOTAL_FALLBACK_WEIGHT_PCT:.4f}% "
            f"({', '.join(fallback_names[:10])})"
        )

    total_live = float(sum(live_values))
    if total_live <= 0:
        raise HoldingsError("VGT: zero total market value after Yahoo repricing")

    out["shares"] = adjusted_shares
    out["market_value"] = live_values
    out["weight"] = pd.Series(live_values, index=out.index) / total_live * 100.0
    out = out.drop(columns=["_yf"])

    if fallback_names:
        print(
            f"[VGT] Tiny fallback tail retained at snapshot value: "
            f"{fallback_weight:.4f}%",
            file=sys.stderr,
        )

    return out


# ---------------------------------------------------------------------------
# IDENTITY / NAMING NORMALISATION
# ---------------------------------------------------------------------------

LEGAL_SUFFIX_PATTERNS = [
    r"\bINCORPORATED\b",
    r"\bINC\b",
    r"\bCORPORATION\b",
    r"\bCORP\b",
    r"\bCOMPANY\b",
    r"\bCO\b",
    r"\bLIMITED\b",
    r"\bLTD\b",
    r"\bPLC\b",
    r"\bLLC\b",
    r"\bHOLDINGS\b",
    r"\bHOLDING\b",
    r"\bHLDGS\b",
    r"\bGROUP\b",
    r"\bGRP\b",
]


def canonical_ticker(raw: str) -> str:
    s = clean_text(raw).upper()
    if not s:
        return ""
    s = re.sub(r"\s+(US|UN|UW|UQ|UR|UF)$", "", s)
    s = s.replace("/", "-")
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-")


def normalized_name(raw: str) -> str:
    s = unicodedata.normalize("NFKD", clean_text(raw))
    s = s.encode("ascii", "ignore").decode()
    s = s.upper()
    s = re.sub(r"[^A-Z0-9& ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Remove common legal suffixes only from the tail so company identity is not
    # over-normalised.
    changed = True
    while changed and s:
        changed = False
        for pat in LEGAL_SUFFIX_PATTERNS:
            new = re.sub(rf"(?:\s+{pat})$", "", s).strip()
            if new != s:
                s = new
                changed = True
    return s


def valid_isin(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", clean_text(value).upper()))


def valid_sedol(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]{7}", clean_text(value).upper()))


def assign_identity_keys(long_df: pd.DataFrame) -> pd.DataFrame:
    df = long_df.copy()
    df["_ticker"] = df["raw_ticker"].map(canonical_ticker)
    df["_name_norm"] = df["raw_name"].map(normalized_name)
    df["_isin"] = df["isin"].map(lambda x: clean_text(x).upper())
    df["_sedol"] = df["sedol"].map(lambda x: clean_text(x).upper())

    # Build ticker -> strong ID aliases only where the ticker maps to exactly one
    # ISIN/SEDOL across all providers. This lets a row lacking ISIN inherit the
    # same identity without blindly trusting globally ambiguous bare tickers.
    ticker_to_strong: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        ticker = row["_ticker"]
        if not ticker:
            continue
        strong = None
        if valid_isin(row["_isin"]):
            strong = "ISIN:" + row["_isin"]
        elif valid_sedol(row["_sedol"]):
            strong = "SEDOL:" + row["_sedol"]
        if strong:
            ticker_to_strong.setdefault(ticker, set()).add(strong)

    unique_ticker_alias = {
        t: next(iter(ids))
        for t, ids in ticker_to_strong.items()
        if len(ids) == 1
    }

    def make_key(row: pd.Series) -> str:
        if valid_isin(row["_isin"]):
            return "ISIN:" + row["_isin"]
        if valid_sedol(row["_sedol"]):
            return "SEDOL:" + row["_sedol"]

        ticker = row["_ticker"]
        if ticker and ticker in unique_ticker_alias:
            return unique_ticker_alias[ticker]
        if ticker:
            # Include normalized name to protect against genuine bare-ticker
            # collisions across international markets.
            return f"TK:{ticker}|{row['_name_norm']}"
        return "NM:" + row["_name_norm"]

    df["_identity"] = df.apply(make_key, axis=1)
    return df


def choose_canonical_name(group: pd.DataFrame) -> str:
    identity = str(group["_identity"].iloc[0])
    tickers = [t for t in group["_ticker"] if t]
    names = [clean_text(n) for n in group["raw_name"] if clean_text(n)]

    # Override by strong identity first, then ticker, then normalized name.
    if identity in CANONICAL_NAME_OVERRIDES:
        return CANONICAL_NAME_OVERRIDES[identity]

    for ticker in tickers:
        key = f"TK:{ticker}"
        if key in CANONICAL_NAME_OVERRIDES:
            return CANONICAL_NAME_OVERRIDES[key]

    for name in names:
        key = normalized_name(name)
        if key in CANONICAL_NAME_OVERRIDES:
            return CANONICAL_NAME_OVERRIDES[key]

    if not names:
        return tickers[0] if tickers else identity

    counts = pd.Series(names).value_counts()
    top_count = int(counts.iloc[0])
    tied = [name for name, count in counts.items() if int(count) == top_count]

    # Deterministic tie-break: prefer a mixed-case display label, then shorter
    # labels (less provider boilerplate), then lexical order.
    def score(name: str):
        mixed_case = not (name.isupper() or name.islower())
        return (0 if mixed_case else 1, len(name), name.upper())

    return sorted(tied, key=score)[0]


def choose_canonical_ticker(group: pd.DataFrame) -> str:
    tickers = [t for t in group["_ticker"] if t]
    if not tickers:
        return ""

    counts = pd.Series(tickers).value_counts()
    top_count = int(counts.iloc[0])
    tied = [t for t, count in counts.items() if int(count) == top_count]

    # Prefer exchange-qualified tickers when available because they avoid
    # international collisions (e.g. 2330.TW).
    qualified = [t for t in tied if "." in t]
    if qualified:
        return sorted(qualified)[0]
    return sorted(tied, key=lambda x: (len(x), x))[0]


def canonicalize_across_etfs(long_df: pd.DataFrame) -> pd.DataFrame:
    df = assign_identity_keys(long_df)

    mapping: dict[str, tuple[str, str]] = {}
    for identity, group in df.groupby("_identity", sort=False):
        mapping[str(identity)] = (
            choose_canonical_ticker(group),
            choose_canonical_name(group),
        )

    df["canonical_ticker"] = df["_identity"].map(lambda k: mapping[str(k)][0])
    df["canonical_name"] = df["_identity"].map(lambda k: mapping[str(k)][1])

    # Fallback to raw values where no canonical ticker/name exists.
    df["canonical_ticker"] = df["canonical_ticker"].where(
        df["canonical_ticker"] != "",
        df["raw_ticker"].map(canonical_ticker),
    )
    df["canonical_name"] = df["canonical_name"].where(
        df["canonical_name"] != "",
        df["raw_name"].map(clean_text),
    )
    return df


# ---------------------------------------------------------------------------
# VALIDATION / OUTPUT
# ---------------------------------------------------------------------------

def validate_live_frame(
    etf: str,
    df: pd.DataFrame,
    previous_count: int | None,
) -> None:
    if df.empty:
        raise HoldingsError(f"{etf}: no holdings")

    hard_floor = MIN_HOLDINGS_BY_TICKER[etf]
    required = hard_floor
    if previous_count and previous_count >= hard_floor:
        required = max(
            required,
            math.floor(previous_count * MIN_PREVIOUS_COUNT_RATIO),
        )

    if len(df) < required:
        raise HoldingsError(
            f"{etf}: only {len(df)} rows; required at least {required}"
        )

    weights = pd.to_numeric(df["weight"], errors="coerce")
    numeric_count = int(weights.notna().sum())
    if numeric_count < math.floor(len(df) * 0.98):
        raise HoldingsError(
            f"{etf}: only {numeric_count}/{len(df)} rows have numeric weights"
        )

    if (weights.dropna() < -0.01).any():
        raise HoldingsError(f"{etf}: unexpected materially negative holding weight")

    total = float(weights.sum(skipna=True))
    if not MIN_TOTAL_WEIGHT <= total <= MAX_TOTAL_WEIGHT:
        raise HoldingsError(
            f"{etf}: weight sum {total:.6f}% outside "
            f"[{MIN_TOTAL_WEIGHT}, {MAX_TOTAL_WEIGHT}]"
        )

    if df["raw_name"].astype(str).str.contains(r"<[^>]+>", regex=True).any():
        raise HoldingsError(f"{etf}: HTML leaked into holding names")

    # Duplicate exact raw rows often indicate repeated HTML widgets or parsing
    # the same table twice.
    duplicated = df.duplicated(
        subset=["raw_ticker", "raw_name", "shares", "market_value"],
        keep=False,
    )
    if int(duplicated.sum()) > max(4, int(len(df) * 0.02)):
        raise HoldingsError(
            f"{etf}: suspicious duplicate-row count {int(duplicated.sum())}"
        )


def load_previous(combined_path: Path) -> dict[str, pd.DataFrame]:
    if not combined_path.exists():
        return {}
    try:
        previous = pd.read_csv(combined_path)
    except Exception as exc:
        print(
            f"WARNING: could not read last-known-good CSV: {exc}",
            file=sys.stderr,
        )
        return {}

    if any(c not in previous.columns for c in OUTPUT_COLS):
        print(
            "WARNING: last-known-good schema mismatch; ignoring previous file",
            file=sys.stderr,
        )
        return {}

    out: dict[str, pd.DataFrame] = {}
    for ticker, group in previous.groupby("fund_ticker", sort=False):
        out[str(ticker).upper()] = group[OUTPUT_COLS].copy().reset_index(drop=True)
    return out


def validate_previous_output(etf: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        raise HoldingsError(f"{etf}: last-known-good rows empty")

    floor = MIN_HOLDINGS_BY_TICKER[etf]
    if len(frame) < floor:
        raise HoldingsError(
            f"{etf}: last-known-good has only {len(frame)} rows (<{floor})"
        )

    weights = pd.to_numeric(frame["weight"], errors="coerce")
    total = float(weights.sum(skipna=True))
    if not MIN_TOTAL_WEIGHT <= total <= MAX_TOTAL_WEIGHT:
        raise HoldingsError(
            f"{etf}: last-known-good weight sum {total:.4f}% invalid"
        )


def normalized_output_for_etf(
    etf: str,
    canonical_df: pd.DataFrame,
    source_date: str,
    retrieved_at: datetime,
) -> pd.DataFrame:
    fund = canonical_df[canonical_df["etf"] == etf].copy()
    fund["weight"] = pd.to_numeric(fund["weight"], errors="coerce")
    fund["shares"] = pd.to_numeric(fund["shares"], errors="coerce")
    fund["market_value"] = pd.to_numeric(
        fund["market_value"], errors="coerce"
    )

    fund = fund.sort_values(
        "weight", ascending=False, na_position="last"
    ).reset_index(drop=True)

    out = pd.DataFrame(
        {
            "source_date": source_date,
            "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
            "fund_ticker": etf,
            "provider": PROVIDER_LABELS[etf],
            "rank": range(1, len(fund) + 1),
            "holding_ticker": fund["canonical_ticker"],
            "holding_name": fund["canonical_name"],
            "weight": fund["weight"],
            "shares_held": fund["shares"],
            "market_value_usd": fund["market_value"],
        }
    )
    return out[OUTPUT_COLS]


def write_csv_atomic(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp = destination.with_name(f".{destination.name}.tmp")
    try:
        frame.to_csv(
            tmp,
            index=False,
            encoding="utf-8-sig",
            float_format="%.15g",
        )
        if not tmp.exists() or tmp.stat().st_size == 0:
            raise HoldingsError(f"Generated empty CSV: {tmp}")
        os.replace(tmp, destination)
    finally:
        if tmp.exists():
            tmp.unlink()


# ---------------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------------

Fetcher = Callable[[requests.Session, str, int], FetchResult]

FETCHERS: dict[str, Fetcher] = {
    "VGT": fetch_vanguard,
    "ACWI": fetch_ishares,
    "XLF": fetch_ssga,
    "XLI": fetch_ssga,
    "XLC": fetch_ssga,
    "PPH": fetch_vaneck,
    "MLPX": fetch_globalx,
    "GRID": fetch_firsttrust,
    "SOXQ": fetch_invesco,
}

DEFAULT_ETFS = ["VGT", "ACWI", "XLF", "XLI", "XLC", "PPH", "MLPX", "GRID", "SOXQ"]


def fetch_all_live(
    session: requests.Session,
    etfs: list[str],
    previous: dict[str, pd.DataFrame],
    timeout: int,
) -> tuple[
    dict[str, pd.DataFrame],
    dict[str, str],
    dict[str, str],
    dict[str, str],
]:
    live: dict[str, pd.DataFrame] = {}
    source_dates: dict[str, str] = {}
    precision_methods: dict[str, str] = {}
    failures: dict[str, str] = {}

    for etf in etfs:
        print(f"[{etf}] Fetching {PROVIDER_LABELS[etf]}...", file=sys.stderr)
        try:
            result = FETCHERS[etf](session, etf, timeout)
            frame = result.frame.copy()

            if etf == "VGT":
                frame = drift_vgt_with_yahoo(frame, result.source_date)
                result.precision_method = "vanguard_shares_x_live_price"

            previous_count = len(previous[etf]) if etf in previous else None
            validate_live_frame(etf, frame, previous_count)

            frame["etf"] = etf
            live[etf] = frame
            source_dates[etf] = result.source_date
            precision_methods[etf] = result.precision_method

            print(
                f"[{etf}] OK: {len(frame)} rows, "
                f"{frame['weight'].sum(skipna=True):.8f}% "
                f"({result.precision_method})",
                file=sys.stderr,
            )
        except Exception as exc:
            failures[etf] = str(exc)
            print(f"[{etf}] LIVE FAILED: {exc}", file=sys.stderr)

    return live, source_dates, precision_methods, failures


def run(
    etfs: list[str],
    out_dir: Path,
    combined_name: str,
    timeout: int,
) -> int:
    combined_path = out_dir / combined_name
    previous = load_previous(combined_path)

    with build_session() as session:
        live, source_dates, precision_methods, live_failures = fetch_all_live(
            session,
            etfs,
            previous,
            timeout,
        )

    # Canonical naming/identity is performed jointly across ALL successful live
    # funds so the same security gets the same display ticker/name across ETFs.
    canonical_live: pd.DataFrame | None = None
    if live:
        stacked = pd.concat(
            [live[e] for e in etfs if e in live],
            ignore_index=True,
        )
        canonical_live = canonicalize_across_etfs(stacked)

    retrieved_at = utc_now()
    final_by_etf: dict[str, pd.DataFrame] = {}
    source_status: dict[str, str] = {}
    fatal: dict[str, str] = {}

    for etf in etfs:
        if etf in live and canonical_live is not None:
            out = normalized_output_for_etf(
                etf,
                canonical_live,
                source_dates[etf],
                retrieved_at,
            )

            # Final output-level sanity check.
            validate_previous_output(etf, out)
            final_by_etf[etf] = out
            source_status[etf] = (
                f"{PROVIDER_LABELS[etf]} / {precision_methods[etf]}"
            )
            continue

        if etf in previous:
            try:
                prev = previous[etf].copy()
                validate_previous_output(etf, prev)
                final_by_etf[etf] = prev[OUTPUT_COLS]
                source_status[etf] = "Last-known-good"
                print(
                    f"[{etf}] Using last-known-good rows; "
                    f"original timestamp preserved",
                    file=sys.stderr,
                )
                continue
            except Exception as exc:
                fatal[etf] = (
                    f"live failed ({live_failures.get(etf, 'unknown')}); "
                    f"previous invalid ({exc})"
                )
        else:
            fatal[etf] = (
                f"live failed ({live_failures.get(etf, 'unknown')}); "
                f"no last-known-good rows"
            )

    if fatal:
        print(
            "\nUpdate aborted. Existing combined CSV was NOT replaced.",
            file=sys.stderr,
        )
        for etf, reason in fatal.items():
            print(f"  - {etf}: {reason}", file=sys.stderr)
        return 1

    combined = pd.concat(
        [final_by_etf[e] for e in etfs],
        ignore_index=True,
    )

    if combined["fund_ticker"].nunique() != len(etfs):
        print(
            "ERROR: combined output does not contain every requested ETF; "
            "existing CSV was not replaced",
            file=sys.stderr,
        )
        return 1

    write_csv_atomic(combined, combined_path)

    print(
        f"\nPublished {len(combined)} rows across {len(etfs)} ETFs -> "
        f"{combined_path}",
        file=sys.stderr,
    )
    print("Sources used:", file=sys.stderr)
    for etf in etfs:
        print(f"  - {etf}: {source_status[etf]}", file=sys.stderr)

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "tickers",
        nargs="*",
        help="Optional ETF tickers. Defaults to the existing 9-fund basket.",
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--combined-name", default=DEFAULT_COMBINED_NAME)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if Path(args.combined_name).name != args.combined_name:
        print(
            "ERROR: --combined-name must be a filename, not a path",
            file=sys.stderr,
        )
        return 2

    if args.timeout <= 0:
        print("ERROR: --timeout must be > 0", file=sys.stderr)
        return 2

    etfs = (
        [t.strip().upper() for t in args.tickers if t.strip()]
        if args.tickers
        else DEFAULT_ETFS.copy()
    )

    # Deduplicate while preserving order.
    etfs = list(dict.fromkeys(etfs))

    unknown = [e for e in etfs if e not in FETCHERS]
    if unknown:
        print(
            f"ERROR: unknown ETF(s): {unknown}. Known: {list(FETCHERS)}",
            file=sys.stderr,
        )
        return 2

    return run(etfs, args.out_dir, args.combined_name, args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
