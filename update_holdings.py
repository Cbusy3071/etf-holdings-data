#!/usr/bin/env python3
"""
GitHub-Actions-friendly ETF holdings puller.

Fallback hierarchy (best -> worst):
    1. Barchart
    2. Vested Finance
    3. FinanceCharts
    4. Zacks
    5. CompaniesMarketCap
    6. Schwab
    7. FindBillion
    8. Existing last-known-good rows from ETF_Holdings_Latest.csv

The script keeps the existing 10-column CSV schema used by Excel/Power Query.
A source is accepted only after completeness and weight validation. HTML markup in
holding names is cleaned before publishing.

Requires:
    pip install requests pandas

Optional:
    Set BARCHART_API_KEY in GitHub Actions secrets to use Barchart's official
    ETF constituents API. Without a key the script attempts the public page and
    falls through safely if that page is blocked or JS-only.
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_OUTPUT_DIR = Path("data")
DEFAULT_COMBINED_NAME = "ETF_Holdings_Latest.csv"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MIN_ROWS = 1

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

PROVIDERS = {
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

# Conservative floors: low enough to tolerate legitimate portfolio changes,
# high enough to reject top-10/top-20/top-100 truncated responses.
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

UNIFIED_COLS = [
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

ZACKS_URL = "https://www.zacks.com/funds/etf/{ticker}/holding"
ZACKS_HOLDINGS_RE = re.compile(
    r"etf_holdings\.formatted_data\s*=\s*(\[.*?\])\s*;",
    flags=re.DOTALL,
)
ZACKS_ETF_SYMBOL_RE = re.compile(r"/funds/etf/([^\"'/?<>&\\]+)", re.I)
ZACKS_REL_SYMBOL_RE = re.compile(r"\brel=[\"']([^\"']+)[\"']", re.I)

COMPANIES_MARKET_CAP_SLUGS = {
    "VGT": "vanguard-information-technology-index-fund-etf-shares",
    "ACWI": "ishares-msci-acwi-etf",
    "XLF": "financial-select-sector-spdr-fund",
    "XLI": "industrial-select-sector-spdr-fund",
    "XLC": "communication-services-select-sector-spdr-fund",
    "PPH": "vaneck-pharmaceutical-etf",
    "MLPX": "global-x-mlp-energy-infrastructure-etf",
    "GRID": "first-trust-nasdaq-clean-edge-smart-grid-infrastructure-index-fund",
    "SOXQ": "invesco-phlx-semiconductor-etf",
}


class HoldingsError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def clean_holding_name(value: Any) -> str:
    """Return plain company/security text, preferring HTML title/aria-label text."""
    if value is None or value is pd.NA:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    text = html_lib.unescape(text)

    # Zacks commonly embeds the full name in title="..." while visible text is
    # truncated. Prefer that authoritative full value.
    for attr in ("title", "aria-label"):
        m = re.search(rf"\b{attr}\s*=\s*([\"'])(.*?)\1", text, flags=re.I | re.S)
        if m and m.group(2).strip():
            return re.sub(r"\s+", " ", html_lib.unescape(m.group(2))).strip()

    # Strip any remaining markup and normalize whitespace.
    text = re.sub(r"<[^>]*>", " ", text)
    text = html_lib.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def numeric_value(value: Any) -> float | None:
    if value is None or value is pd.NA:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"-", "--", "N/A", "NA", "NONE", "NAN"}:
        return None
    negative = text.startswith("(") and text.endswith(")")
    text = text.replace(",", "").replace("%", "").replace("$", "").strip("() ")
    # Remove footnotes while retaining numeric/scientific notation characters.
    text = re.sub(r"[^0-9eE+\-.]", "", text)
    if not text:
        return None
    try:
        result = float(text)
        return -result if negative else result
    except ValueError:
        return None


def numeric_series(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values.map(numeric_value), errors="coerce")


def normalize_header(value: Any) -> str:
    text = clean_holding_name(value).lower()
    text = text.replace("%", " percent ")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


class SimpleTableParser(HTMLParser):
    """Small stdlib HTML table parser; avoids adding BeautifulSoup/lxml deps."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None
        self._cell_preferred: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell_parts = []
            self._cell_preferred = None
        elif self._cell_parts is not None:
            attr_dict = {k.lower(): v for k, v in attrs if v is not None}
            for key in ("title", "aria-label"):
                if attr_dict.get(key):
                    self._cell_preferred = str(attr_dict[key])
                    break

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._cell_parts is not None and self._row is not None:
            text = self._cell_preferred or " ".join(self._cell_parts)
            self._row.append(re.sub(r"\s+", " ", text).strip())
            self._cell_parts = None
            self._cell_preferred = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            if any(cell.strip() for cell in self._row):
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            if self._table:
                self.tables.append(self._table)
            self._table = None


def parse_html_tables(page_text: str) -> list[list[list[str]]]:
    parser = SimpleTableParser()
    parser.feed(page_text)
    return parser.tables


TICKER_ALIASES = {
    "ticker", "symbol", "holding ticker", "stock ticker", "constituent ticker"
}
NAME_ALIASES = {
    "name", "company", "company name", "holding", "holding name", "security",
    "security name", "constituent", "stock", "stock name"
}
WEIGHT_ALIASES = {
    "weight", "weight percent", "percent", "ratio", "portfolio weight",
    "holding percent", "percent assets", "assets percent", "of assets",
    "percent of assets", "percent of portfolio", "portfolio percent"
}
SHARES_ALIASES = {
    "shares", "shares held", "share held", "shares owned", "quantity", "sharesheld"
}
MARKET_VALUE_ALIASES = {
    "market value", "market value usd", "value", "marketvalue"
}


def find_column(headers: list[str], aliases: set[str], contains: tuple[str, ...] = ()) -> int | None:
    normalized = [normalize_header(h) for h in headers]
    for i, h in enumerate(normalized):
        if h in aliases:
            return i
    for i, h in enumerate(normalized):
        if any(token in h for token in contains):
            return i
    return None


def table_to_holdings(table: list[list[str]], ticker: str, source: str) -> pd.DataFrame | None:
    if len(table) < 2:
        return None

    # Try first few rows as possible headers because some sites add title rows.
    for header_idx in range(min(4, len(table) - 1)):
        headers = table[header_idx]
        ticker_col = find_column(headers, TICKER_ALIASES, ("ticker", "symbol"))
        name_col = find_column(headers, NAME_ALIASES, ("company", "security", "holding name", "stock name"))
        weight_col = find_column(headers, WEIGHT_ALIASES, ("weight", "ratio", "portfolio", "assets"))
        shares_col = find_column(headers, SHARES_ALIASES, ("shares",))
        value_col = find_column(headers, MARKET_VALUE_ALIASES, ("market value",))

        if ticker_col is None or weight_col is None:
            continue

        records: list[dict[str, Any]] = []
        for row in table[header_idx + 1 :]:
            if max(ticker_col, weight_col) >= len(row):
                continue
            holding_ticker = clean_holding_name(row[ticker_col]).upper().strip()
            # Reject obvious headers/labels accidentally repeated inside the body.
            if not holding_ticker or normalize_header(holding_ticker) in TICKER_ALIASES:
                continue
            weight = numeric_value(row[weight_col])
            if weight is None:
                # retain genuine non-equity rows only when a ticker exists; validation
                # later ensures the overall source still has enough numeric weights.
                pass
            name = clean_holding_name(row[name_col]) if name_col is not None and name_col < len(row) else ""
            shares = numeric_value(row[shares_col]) if shares_col is not None and shares_col < len(row) else None
            market_value = numeric_value(row[value_col]) if value_col is not None and value_col < len(row) else None
            records.append(
                {
                    "holding_ticker": holding_ticker,
                    "holding_name": name,
                    "weight": weight,
                    "shares_held": shares,
                    "market_value_usd": market_value,
                }
            )

        if records:
            frame = pd.DataFrame.from_records(records)
            frame = frame[frame["holding_ticker"].astype(str).str.len() > 0].copy()
            if not frame.empty:
                return frame
    return None


def parse_best_html_table(page_text: str, ticker: str, source: str) -> pd.DataFrame:
    candidates: list[pd.DataFrame] = []
    for table in parse_html_tables(page_text):
        frame = table_to_holdings(table, ticker, source)
        if frame is not None:
            candidates.append(frame)
    if not candidates:
        raise HoldingsError(f"No holdings table found in {source} HTML for {ticker}")
    # Prefer the largest candidate; holdings tables should dominate top-10 widgets.
    return max(candidates, key=len).reset_index(drop=True)


def fetch_html_source(
    session: requests.Session,
    ticker: str,
    timeout_seconds: int,
    source: str,
    url: str,
) -> pd.DataFrame:
    response = session.get(url, timeout=(10, timeout_seconds), allow_redirects=True)
    response.raise_for_status()
    return parse_best_html_table(response.text, ticker, source)


def extract_zacks_symbol(symbol_html: Any) -> str:
    text = "" if symbol_html is None else str(symbol_html)
    m = ZACKS_ETF_SYMBOL_RE.search(text)
    if m:
        return m.group(1).strip().upper()
    m = ZACKS_REL_SYMBOL_RE.search(text)
    if m:
        candidate = m.group(1).strip().upper()
        if candidate and " " not in candidate and len(candidate) <= 30:
            return candidate
    plain = clean_holding_name(text).upper()
    if re.fullmatch(r"[A-Z0-9.\-]{1,30}", plain):
        return plain
    return ""


def fetch_zacks(session: requests.Session, ticker: str, timeout_seconds: int) -> pd.DataFrame:
    url = ZACKS_URL.format(ticker=ticker)
    response = session.get(
        url,
        timeout=(10, timeout_seconds),
        headers={"Referer": f"https://www.zacks.com/funds/etf/{ticker}"},
    )
    response.raise_for_status()
    match = ZACKS_HOLDINGS_RE.search(response.text)
    if not match:
        raise HoldingsError(f"Zacks holdings payload not found for {ticker}")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise HoldingsError(f"Invalid Zacks holdings JSON for {ticker}: {exc}") from exc

    records: list[dict[str, Any]] = []
    for row in payload if isinstance(payload, list) else []:
        if not isinstance(row, list) or len(row) < 4:
            continue
        holding_ticker = extract_zacks_symbol(row[1])
        if not holding_ticker:
            continue
        records.append(
            {
                "holding_ticker": holding_ticker,
                "holding_name": clean_holding_name(row[0]),
                "weight": numeric_value(row[3]),
                "shares_held": numeric_value(row[2]),
                "market_value_usd": None,
            }
        )
    if not records:
        raise HoldingsError(f"No usable Zacks rows for {ticker}")
    return pd.DataFrame.from_records(records)


def fetch_barchart(session: requests.Session, ticker: str, timeout_seconds: int) -> pd.DataFrame:
    api_key = os.getenv("BARCHART_API_KEY", "").strip()
    if api_key:
        url = "https://ondemand.websol.barchart.com/getETFConstituents.json"
        response = session.get(
            url,
            params={"apikey": api_key, "symbol": ticker},
            timeout=(10, timeout_seconds),
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(rows, list) or not rows:
            raise HoldingsError(f"Barchart API returned no constituents for {ticker}")
        frame = pd.DataFrame(rows).rename(
            columns={
                "symbol": "holding_ticker",
                "name": "holding_name",
                "percent": "weight",
                "weight": "weight",
                "shares": "shares_held",
                "marketValue": "market_value_usd",
            }
        )
        if "holding_ticker" not in frame or "weight" not in frame:
            raise HoldingsError(f"Unexpected Barchart API schema for {ticker}")
        for col in ("holding_name", "shares_held", "market_value_usd"):
            if col not in frame:
                frame[col] = pd.NA
        return frame[["holding_ticker", "holding_name", "weight", "shares_held", "market_value_usd"]]

    return fetch_html_source(
        session,
        ticker,
        timeout_seconds,
        "Barchart",
        f"https://www.barchart.com/etfs-funds/quotes/{ticker}/constituents?orderBy=percent&orderDir=desc",
    )


def fetch_vested(session: requests.Session, ticker: str, timeout_seconds: int) -> pd.DataFrame:
    # The short ETF route is allowed to redirect to Vested's SEO slug URL.
    return fetch_html_source(
        session,
        ticker,
        timeout_seconds,
        "Vested",
        f"https://vestedfinance.com/us-stocks/etf/{ticker.lower()}/",
    )


def fetch_financecharts(session: requests.Session, ticker: str, timeout_seconds: int) -> pd.DataFrame:
    return fetch_html_source(
        session,
        ticker,
        timeout_seconds,
        "FinanceCharts",
        f"https://www.financecharts.com/etfs/{ticker}/holdings",
    )


def fetch_companiesmarketcap(session: requests.Session, ticker: str, timeout_seconds: int) -> pd.DataFrame:
    slug = COMPANIES_MARKET_CAP_SLUGS.get(ticker)
    if not slug:
        raise HoldingsError(f"No CompaniesMarketCap slug configured for {ticker}")
    return fetch_html_source(
        session,
        ticker,
        timeout_seconds,
        "CompaniesMarketCap",
        f"https://companiesmarketcap.com/{slug}/holdings/",
    )


def fetch_schwab(session: requests.Session, ticker: str, timeout_seconds: int) -> pd.DataFrame:
    return fetch_html_source(
        session,
        ticker,
        timeout_seconds,
        "Schwab",
        "https://www.schwab.wallst.com/schwab/Prospect/research/etfs/schwabETF/"
        f"index.asp?type=holdings&symbol={ticker}",
    )


def fetch_findbillion(session: requests.Session, ticker: str, timeout_seconds: int) -> pd.DataFrame:
    return fetch_html_source(
        session,
        ticker,
        timeout_seconds,
        "FindBillion",
        f"https://www.findbillion.com/en/usstock/etf/{ticker}/hold",
    )


def standardize_raw(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    for col in ("holding_ticker", "holding_name", "weight", "shares_held", "market_value_usd"):
        if col not in frame.columns:
            frame[col] = pd.NA

    frame["holding_ticker"] = frame["holding_ticker"].fillna("").astype(str).str.strip().str.upper()
    frame["holding_name"] = frame["holding_name"].map(clean_holding_name)
    frame["weight"] = numeric_series(frame["weight"])
    frame["shares_held"] = numeric_series(frame["shares_held"])
    frame["market_value_usd"] = numeric_series(frame["market_value_usd"])

    # Do NOT deduplicate by bare ticker. International exchanges can legitimately
    # reuse the same ticker (AI, MC, SU, numeric local codes, etc.).
    frame = frame[frame["holding_ticker"].str.len() > 0].copy()
    frame = frame.sort_values("weight", ascending=False, na_position="last").reset_index(drop=True)
    return frame[["holding_ticker", "holding_name", "weight", "shares_held", "market_value_usd"]]


def normalize(frame: pd.DataFrame, fund_ticker: str, retrieved_at: datetime) -> pd.DataFrame:
    frame = standardize_raw(frame)
    output = pd.DataFrame(
        {
            "source_date": retrieved_at.date().isoformat(),
            "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
            "fund_ticker": fund_ticker,
            "provider": PROVIDERS.get(fund_ticker, ""),
            "holding_ticker": frame["holding_ticker"],
            "holding_name": frame["holding_name"],
            "weight": frame["weight"],
            "shares_held": frame["shares_held"],
            "market_value_usd": frame["market_value_usd"],
        }
    )
    output = output.sort_values("weight", ascending=False, na_position="last").reset_index(drop=True)
    output.insert(4, "rank", range(1, len(output) + 1))
    return output[UNIFIED_COLS]


def validate_normalized(
    frame: pd.DataFrame,
    ticker: str,
    user_min_rows: int,
    previous_count: int | None = None,
) -> None:
    if frame.empty:
        raise HoldingsError("No rows")
    missing = [c for c in UNIFIED_COLS if c not in frame.columns]
    if missing:
        raise HoldingsError(f"Missing output columns: {missing}")

    required_floor = max(user_min_rows, MIN_HOLDINGS_BY_TICKER.get(ticker, user_min_rows))
    if previous_count and previous_count >= required_floor:
        # Refuse a major sudden truncation relative to the last valid snapshot.
        required_floor = max(required_floor, math.floor(previous_count * 0.85))

    if len(frame) < required_floor:
        raise HoldingsError(f"Only {len(frame)} rows; required at least {required_floor}")

    numeric_weights = frame["weight"].notna().sum()
    if numeric_weights < max(1, int(len(frame) * 0.85)):
        raise HoldingsError(
            f"Only {numeric_weights}/{len(frame)} rows have numeric weights"
        )

    total_weight = float(frame["weight"].sum(skipna=True))
    if not 85.0 <= total_weight <= 115.0:
        raise HoldingsError(
            f"Weight sum {total_weight:.2f}% is inconsistent with a complete portfolio"
        )

    # Raw HTML should never reach the live CSV now.
    bad_html = frame["holding_name"].astype(str).str.contains(r"<[^>]+>", regex=True, na=False).sum()
    if bad_html:
        raise HoldingsError(f"{bad_html} holding names still contain HTML markup")


def load_previous(combined_path: Path) -> dict[str, pd.DataFrame]:
    if not combined_path.exists():
        return {}
    try:
        previous = pd.read_csv(combined_path)
    except Exception as exc:
        print(f"WARNING: Could not read last-known-good CSV: {exc}", file=sys.stderr)
        return {}
    if any(c not in previous.columns for c in UNIFIED_COLS):
        print("WARNING: Last-known-good CSV schema does not match; ignoring it.", file=sys.stderr)
        return {}
    out: dict[str, pd.DataFrame] = {}
    for ticker, group in previous.groupby("fund_ticker", sort=False):
        out[str(ticker).upper()] = group[UNIFIED_COLS].copy().reset_index(drop=True)
    return out


Fetcher = Callable[[requests.Session, str, int], pd.DataFrame]
SOURCE_CHAIN: list[tuple[str, Fetcher]] = [
    ("Barchart", fetch_barchart),
    ("Vested", fetch_vested),
    ("FinanceCharts", fetch_financecharts),
    ("Zacks", fetch_zacks),
    ("CompaniesMarketCap", fetch_companiesmarketcap),
    ("Schwab", fetch_schwab),
    ("FindBillion", fetch_findbillion),
]


def fetch_with_fallback(
    session: requests.Session,
    ticker: str,
    timeout_seconds: int,
    min_rows: int,
    previous: pd.DataFrame | None,
) -> tuple[pd.DataFrame, str]:
    previous_count = len(previous) if previous is not None else None
    errors: list[str] = []

    for source_name, fetcher in SOURCE_CHAIN:
        print(f"[{ticker}] Trying {source_name}...", file=sys.stderr)
        try:
            raw = fetcher(session, ticker, timeout_seconds)
            retrieved_at = utc_now()  # per-fund/source success timestamp
            normalised = normalize(raw, ticker, retrieved_at)
            validate_normalized(normalised, ticker, min_rows, previous_count)
            return normalised, source_name
        except Exception as exc:
            errors.append(f"{source_name}: {exc}")
            print(f"[{ticker}] {source_name} failed: {exc}", file=sys.stderr)

    if previous is not None:
        # Preserve old provenance; never restamp stale rows as today's data.
        previous_clean = previous.copy()
        previous_clean["holding_name"] = previous_clean["holding_name"].map(clean_holding_name)
        validate_normalized(previous_clean, ticker, min_rows, None)
        return previous_clean[UNIFIED_COLS], "Last-known-good"

    raise HoldingsError(" | ".join(errors))


def write_csv_atomic(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    try:
        frame.to_csv(
            temporary,
            index=False,
            encoding="utf-8-sig",
            float_format="%.15g",
        )
        if not temporary.exists() or temporary.stat().st_size == 0:
            raise HoldingsError(f"Generated empty CSV: {temporary}")
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def unique_tickers(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        ticker = value.strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            out.append(ticker)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tickers", nargs="+", help="ETF tickers, e.g. VGT ACWI XLF")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--combined-name", default=DEFAULT_COMBINED_NAME)
    parser.add_argument("--no-per-fund", action="store_true")
    parser.add_argument("--allow-partial", action="store_true")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--min-rows", type=int, default=DEFAULT_MIN_ROWS)
    parser.add_argument(
        "--no-last-known-good",
        action="store_true",
        help="Do not reuse previous valid fund rows when all live sources fail",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tickers = unique_tickers(args.tickers)
    if not tickers:
        print("ERROR: No valid ETF tickers supplied.", file=sys.stderr)
        return 2
    if args.timeout <= 0 or args.min_rows <= 0:
        print("ERROR: --timeout and --min-rows must be > 0.", file=sys.stderr)
        return 2
    if Path(args.combined_name).name != args.combined_name:
        print("ERROR: --combined-name must be a filename, not a path.", file=sys.stderr)
        return 2

    output_dir: Path = args.out_dir
    combined_path = output_dir / args.combined_name
    previous_by_ticker = {} if args.no_last_known_good else load_previous(combined_path)

    successful: dict[str, pd.DataFrame] = {}
    sources_used: dict[str, str] = {}
    failures: dict[str, str] = {}

    with build_session() as session:
        for ticker in tickers:
            chain = " -> ".join(name for name, _ in SOURCE_CHAIN) + " -> last-known-good"
            print(f"[{ticker}] Fetching holdings ({chain})", file=sys.stderr)
            try:
                frame, source = fetch_with_fallback(
                    session,
                    ticker,
                    args.timeout,
                    args.min_rows,
                    previous_by_ticker.get(ticker),
                )
                successful[ticker] = frame
                sources_used[ticker] = source
                print(
                    f"[{ticker}] OK via {source}: {len(frame)} rows, "
                    f"weight sum {frame['weight'].sum(skipna=True):.2f}%",
                    file=sys.stderr,
                )
            except Exception as exc:
                failures[ticker] = str(exc)
                print(f"[{ticker}] FAILED: {exc}", file=sys.stderr)

    if failures and not args.allow_partial:
        print("\nUpdate aborted. Existing CSV was not replaced.", file=sys.stderr)
        for ticker, reason in failures.items():
            print(f"  - {ticker}: {reason}", file=sys.stderr)
        return 1
    if not successful:
        print("ERROR: No ETF holdings available; nothing written.", file=sys.stderr)
        return 1

    combined = pd.concat([successful[t] for t in tickers if t in successful], ignore_index=True)
    if combined["fund_ticker"].nunique() != len(successful):
        print("ERROR: Combined-output fund count mismatch.", file=sys.stderr)
        return 1

    try:
        if not args.no_per_fund:
            for ticker in tickers:
                if ticker in successful:
                    write_csv_atomic(successful[ticker], output_dir / f"{ticker}_holdings_latest.csv")
        write_csv_atomic(combined, combined_path)
    except Exception as exc:
        print(f"ERROR: Could not write output CSV: {exc}", file=sys.stderr)
        return 1

    print(
        f"\nCombined stream: {len(combined)} rows across "
        f"{combined['fund_ticker'].nunique()} funds -> {combined_path}",
        file=sys.stderr,
    )
    print("Sources used:", file=sys.stderr)
    for ticker in tickers:
        if ticker in sources_used:
            print(f"  - {ticker}: {sources_used[ticker]}", file=sys.stderr)

    if failures:
        print(
            "WARNING: Partial output published because --allow-partial was set. "
            f"Failed funds: {', '.join(failures)}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())