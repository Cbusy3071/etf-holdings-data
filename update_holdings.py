#!/usr/bin/env python3
"""
GitHub-Actions-friendly ETF holdings puller.

Source order for every requested US-listed ETF:
    1. Zacks holdings page (primary)
    2. Perplexity Finance undocumented holdings endpoint (fallback)

The script normalises holdings into one stable CSV schema, validates that every
requested fund succeeded from at least one source, and only then atomically
replaces the live output file. This prevents a failed or partial weekly run from
silently replacing a valid CSV used by Excel.

Default output:
    data/ETF_Holdings_Latest.csv

Examples:
    python scrape_etf_holdings_zacks_primary.py VGT ACWI XLF

    python scrape_etf_holdings_zacks_primary.py \
        --no-per-fund \
        VGT ACWI XLF XLI XLC PPH MLPX GRID SOXQ

    python scrape_etf_holdings_zacks_primary.py \
        --out-dir data \
        --combined-name ETF_Holdings_Latest.csv \
        --no-per-fund \
        VGT ACWI

Requires:
    pip install requests pandas

Important:
    Zacks primary URL pattern:
        https://www.zacks.com/funds/etf/<TICKER>/holding

    Zacks embeds its holdings table data in the page source as the JavaScript
    variable ``etf_holdings.formatted_data``. The parser extracts that JSON
    payload without needing a browser or BeautifulSoup.

    Perplexity fallback endpoint:
        https://www.perplexity.ai/rest/finance/holdings/<TICKER>

    The Perplexity endpoint is undocumented and may change or be disabled.
    Zacks may also change its HTML/JavaScript structure or block automated
    requests. If Zacks cannot be fetched or parsed for a ticker, the script
    automatically attempts Perplexity for that ticker.

    The source_date field is the date this script retrieved the data; it is not
    represented as the issuer's official holdings effective date.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

ZACKS_URL = "https://www.zacks.com/funds/etf/{ticker}/holding"
PERPLEXITY_URL = "https://www.perplexity.ai/rest/finance/holdings/{ticker}"

# Zacks stores the displayed holdings as a JSON-compatible JavaScript array.
# DOTALL makes this tolerant of the array being written across multiple lines.
ZACKS_HOLDINGS_RE = re.compile(
    r"etf_holdings\.formatted_data\s*=\s*(\[.*?\])\s*;",
    flags=re.DOTALL,
)

# Symbol links inside the Zacks payload have historically included both
# /funds/etf/<SYMBOL> and a rel="<SYMBOL>" attribute. Support both forms.
ZACKS_ETF_SYMBOL_RE = re.compile(r"/funds/etf/([^\"'/?<>&\\]+)", flags=re.IGNORECASE)
ZACKS_REL_SYMBOL_RE = re.compile(r"\brel=[\"']([^\"']+)[\"']", flags=re.IGNORECASE)


# Cosmetic only. Add or change entries without affecting the fetch logic.
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


# Keep this schema unchanged so existing Excel / Power Query steps continue to
# work. Source choice is reported in the GitHub Actions log rather than adding a
# new CSV column.
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


class HoldingsError(RuntimeError):
    """Raised when holdings cannot be safely collected or validated."""


def build_session() -> requests.Session:
    """Create a requests session with retries for temporary HTTP failures."""
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


def numeric_series(values: pd.Series) -> pd.Series:
    """Convert display-formatted numeric values such as '1,234' or '4.5%' to numbers."""
    cleaned = (
        values.astype("string")
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.strip()
        .replace({"": pd.NA, "-": pd.NA, "--": pd.NA, "N/A": pd.NA, "NA": pd.NA})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def extract_zacks_symbol(symbol_html: Any) -> str:
    """Extract a holding ticker from the HTML fragment embedded by Zacks."""
    text = "" if symbol_html is None else str(symbol_html)

    match = ZACKS_ETF_SYMBOL_RE.search(text)
    if match:
        return match.group(1).strip().upper()

    match = ZACKS_REL_SYMBOL_RE.search(text)
    if match:
        candidate = match.group(1).strip().upper()
        # Avoid accidentally treating descriptive rel values as symbols.
        if candidate and " " not in candidate and len(candidate) <= 20:
            return candidate

    # Some payload variants may eventually expose a plain symbol rather than an
    # HTML fragment. Accept a conservative ticker-like token as a final option.
    plain = text.strip().upper()
    if re.fullmatch(r"[A-Z0-9.\-]{1,20}", plain):
        return plain

    return ""


def extract_zacks_rows(page_text: str, ticker: str) -> list[list[Any]]:
    """Extract the JSON rows stored in Zacks' etf_holdings.formatted_data variable."""
    match = ZACKS_HOLDINGS_RE.search(page_text)
    if not match:
        preview = re.sub(r"\s+", " ", page_text[:250]).strip()
        raise HoldingsError(
            f"Zacks holdings payload not found for {ticker}. "
            f"Page preview: {preview!r}"
        )

    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise HoldingsError(f"Could not parse Zacks holdings JSON for {ticker}: {exc}") from exc

    if not isinstance(payload, list) or not payload:
        raise HoldingsError(f"No holdings returned by Zacks for {ticker}")

    rows = [row for row in payload if isinstance(row, list)]
    if not rows:
        raise HoldingsError(f"Malformed Zacks holdings rows returned for {ticker}")

    return rows


def fetch_zacks(
    session: requests.Session,
    ticker: str,
    timeout_seconds: int,
) -> pd.DataFrame:
    """Fetch and standardise holdings from the Zacks ETF holdings page."""
    ticker = ticker.strip().upper()
    url = ZACKS_URL.format(ticker=ticker)

    response = session.get(
        url,
        timeout=(10, timeout_seconds),
        headers={"Referer": f"https://www.zacks.com/funds/etf/{ticker}"},
    )
    response.raise_for_status()

    # A 200 response can still be an anti-bot/challenge page. Requiring the
    # holdings variable below makes that fail cleanly into the fallback path.
    rows = extract_zacks_rows(response.text, ticker)

    records: list[dict[str, Any]] = []
    for row in rows:
        # Historically:
        #   [0] company name
        #   [1] symbol HTML
        #   [2] shares held
        #   [3] weight (%)
        #   [4] 52-week % change
        #   [5] report link
        if len(row) < 4:
            continue

        holding_ticker = extract_zacks_symbol(row[1])
        holding_name = "" if row[0] is None else str(row[0]).strip()
        shares_held = row[2]
        weight = row[3]

        # Zacks sometimes contains non-security rows. A usable portfolio row
        # must have at least a symbol and a weight.
        if not holding_ticker:
            continue

        records.append(
            {
                "holding_ticker": holding_ticker,
                "holding_name": holding_name,
                "weight": weight,
                "shares_held": shares_held,
                # Zacks' displayed holdings array does not provide market value.
                "market_value_usd": pd.NA,
            }
        )

    if not records:
        raise HoldingsError(f"No usable Zacks holding rows parsed for {ticker}")

    frame = pd.DataFrame.from_records(records)
    frame["weight"] = numeric_series(frame["weight"])
    frame["shares_held"] = numeric_series(frame["shares_held"])

    if frame["weight"].notna().sum() == 0:
        raise HoldingsError(f"No numeric holding weights returned by Zacks for {ticker}")

    frame["holding_ticker"] = frame["holding_ticker"].fillna("").astype(str).str.strip()
    frame["holding_name"] = frame["holding_name"].fillna("").astype(str).str.strip()

    # Drop duplicate symbols defensively. If Zacks ever repeats a symbol, keep
    # the first/highest weight row rather than double-counting the exposure.
    frame = frame.sort_values("weight", ascending=False, na_position="last")
    frame = frame.drop_duplicates(subset=["holding_ticker"], keep="first")
    frame = frame.reset_index(drop=True)
    return frame


def extract_holdings_payload(payload: Any, ticker: str) -> list[dict[str, Any]]:
    """Return a list of holdings from the currently observed Perplexity payload."""
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = None
        for key in ("holdings", "data", "results"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                records = candidate
                break
        if records is None:
            raise HoldingsError(
                f"Unexpected JSON structure returned by Perplexity for {ticker}; "
                f"top-level keys: {sorted(payload.keys())[:10]}"
            )
    else:
        raise HoldingsError(
            f"Unexpected JSON type returned by Perplexity for {ticker}: "
            f"{type(payload).__name__}"
        )

    if not records:
        raise HoldingsError(f"No holdings returned by Perplexity for {ticker}")

    if not all(isinstance(item, dict) for item in records):
        raise HoldingsError(f"Malformed Perplexity holdings records returned for {ticker}")

    return records


def fetch_perplexity(
    session: requests.Session,
    ticker: str,
    timeout_seconds: int,
) -> pd.DataFrame:
    """Fetch and minimally standardise a fund's Perplexity holdings response."""
    ticker = ticker.strip().upper()
    url = PERPLEXITY_URL.format(ticker=ticker)

    response = session.get(
        url,
        timeout=(10, timeout_seconds),
        headers={"Accept": "application/json, text/plain, */*"},
    )
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        preview = response.text[:200].replace("\n", " ")
        raise HoldingsError(
            f"Non-JSON response returned by Perplexity for {ticker}: {preview!r}"
        ) from exc

    records = extract_holdings_payload(payload, ticker)
    frame = pd.DataFrame.from_records(records)

    # The currently observed endpoint uses symbol, name, shares, weight and
    # market_value. Missing optional fields are created below; weight is the
    # only field required for a usable portfolio-weights dataset.
    frame = frame.rename(
        columns={
            "symbol": "holding_ticker",
            "ticker": "holding_ticker",
            "name": "holding_name",
            "shares": "shares_held",
            "marketValue": "market_value_usd",
            "market_value": "market_value_usd",
        }
    )

    if "weight" not in frame.columns:
        raise HoldingsError(f"Required 'weight' field missing from Perplexity for {ticker}")

    for column in ("holding_ticker", "holding_name", "shares_held", "market_value_usd"):
        if column not in frame.columns:
            frame[column] = pd.NA

    frame["weight"] = pd.to_numeric(frame["weight"], errors="coerce")
    frame["shares_held"] = pd.to_numeric(frame["shares_held"], errors="coerce")
    frame["market_value_usd"] = pd.to_numeric(
        frame["market_value_usd"], errors="coerce"
    )

    if frame["weight"].notna().sum() == 0:
        raise HoldingsError(f"No numeric holding weights returned by Perplexity for {ticker}")

    frame["holding_ticker"] = frame["holding_ticker"].fillna("").astype(str).str.strip()
    frame["holding_name"] = frame["holding_name"].fillna("").astype(str).str.strip()

    frame = frame.sort_values(
        "weight", ascending=False, na_position="last"
    ).reset_index(drop=True)
    return frame


def normalize(
    frame: pd.DataFrame,
    fund_ticker: str,
    retrieved_at: datetime,
) -> pd.DataFrame:
    """Map a fund's holdings onto the stable combined-output schema."""
    fund_ticker = fund_ticker.upper()

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

    output = output.sort_values(
        "weight", ascending=False, na_position="last"
    ).reset_index(drop=True)
    output.insert(4, "rank", range(1, len(output) + 1))
    return output[UNIFIED_COLS]


def validate_normalized(
    frame: pd.DataFrame,
    ticker: str,
    min_rows: int,
) -> None:
    """Fail clearly when a fund's normalised output is not safe to publish."""
    missing_columns = [column for column in UNIFIED_COLS if column not in frame.columns]
    if missing_columns:
        raise HoldingsError(
            f"Normalised output for {ticker} is missing columns: {missing_columns}"
        )

    if len(frame) < min_rows:
        raise HoldingsError(
            f"Only {len(frame)} holdings returned for {ticker}; "
            f"minimum required is {min_rows}"
        )

    if frame["fund_ticker"].nunique() != 1 or frame["fund_ticker"].iloc[0] != ticker:
        raise HoldingsError(f"Fund ticker validation failed for {ticker}")

    if frame["weight"].notna().sum() == 0:
        raise HoldingsError(f"No numeric weights survived normalisation for {ticker}")


def fetch_with_fallback(
    session: requests.Session,
    ticker: str,
    timeout_seconds: int,
    retrieved_at: datetime,
    min_rows: int,
) -> tuple[pd.DataFrame, str]:
    """Try Zacks first, then Perplexity if Zacks fails validation for this ticker."""
    attempts = (
        ("Zacks", fetch_zacks),
        ("Perplexity", fetch_perplexity),
    )
    errors: list[str] = []

    for source_name, fetcher in attempts:
        print(f"[{ticker}] Trying {source_name}...", file=sys.stderr)
        try:
            raw = fetcher(session, ticker, timeout_seconds)
            normalised = normalize(raw, ticker, retrieved_at)
            validate_normalized(normalised, ticker, min_rows)
            return normalised, source_name
        except Exception as exc:
            errors.append(f"{source_name}: {exc}")
            print(f"[{ticker}] {source_name} failed: {exc}", file=sys.stderr)

    raise HoldingsError(" | ".join(errors))


def write_csv_atomic(frame: pd.DataFrame, destination: Path) -> None:
    """Write a CSV completely, then atomically replace the destination."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")

    try:
        frame.to_csv(
            temporary,
            index=False,
            encoding="utf-8-sig",
            float_format="%.10g",
        )
        if temporary.stat().st_size == 0:
            raise HoldingsError(f"Generated empty CSV: {temporary}")
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def unique_tickers(values: list[str]) -> list[str]:
    """Normalise tickers and remove duplicates while preserving order."""
    seen: set[str] = set()
    result: list[str] = []

    for value in values:
        ticker = value.strip().upper()
        if not ticker:
            continue
        if ticker not in seen:
            seen.add(ticker)
            result.append(ticker)

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "tickers",
        nargs="+",
        help="ETF tickers to fetch, e.g. VGT ACWI XLF",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--combined-name",
        default=DEFAULT_COMBINED_NAME,
        help=f"Combined CSV filename (default: {DEFAULT_COMBINED_NAME})",
    )
    parser.add_argument(
        "--no-per-fund",
        action="store_true",
        help="Only write the combined live CSV",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help=(
            "Publish successfully fetched funds even if another requested fund fails. "
            "Not recommended for the weekly PM workflow."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"HTTP read timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS})",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=DEFAULT_MIN_ROWS,
        help=f"Minimum holdings required per fund (default: {DEFAULT_MIN_ROWS})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tickers = unique_tickers(args.tickers)

    if not tickers:
        print("ERROR: No valid ETF tickers supplied.", file=sys.stderr)
        return 2
    if args.timeout <= 0:
        print("ERROR: --timeout must be greater than zero.", file=sys.stderr)
        return 2
    if args.min_rows <= 0:
        print("ERROR: --min-rows must be greater than zero.", file=sys.stderr)
        return 2
    if Path(args.combined_name).name != args.combined_name:
        print("ERROR: --combined-name must be a filename, not a path.", file=sys.stderr)
        return 2

    retrieved_at = datetime.now(timezone.utc)
    successful: dict[str, pd.DataFrame] = {}
    sources_used: dict[str, str] = {}
    failures: dict[str, str] = {}

    with build_session() as session:
        for ticker in tickers:
            print(f"[{ticker}] Fetching holdings (Zacks -> Perplexity)...", file=sys.stderr)
            try:
                normalised, source_name = fetch_with_fallback(
                    session=session,
                    ticker=ticker,
                    timeout_seconds=args.timeout,
                    retrieved_at=retrieved_at,
                    min_rows=args.min_rows,
                )
                successful[ticker] = normalised
                sources_used[ticker] = source_name

                top_weight = normalised["weight"].dropna().iloc[0]
                print(
                    f"[{ticker}] OK via {source_name}: {len(normalised)} rows "
                    f"(top holding weight {top_weight:.4g})",
                    file=sys.stderr,
                )
            except Exception as exc:  # one failed ticker is reported with context
                failures[ticker] = str(exc)
                print(f"[{ticker}] FAILED from all sources: {exc}", file=sys.stderr)

    if failures and not args.allow_partial:
        print("\nUpdate aborted. Existing CSV was not replaced.", file=sys.stderr)
        for ticker, reason in failures.items():
            print(f"  - {ticker}: {reason}", file=sys.stderr)
        return 1

    if not successful:
        print("ERROR: No ETF holdings were collected; nothing was written.", file=sys.stderr)
        return 1

    # All validation is complete before any live output is replaced.
    combined = pd.concat(
        [successful[ticker] for ticker in tickers if ticker in successful],
        ignore_index=True,
    )

    expected_funds = len(successful)
    actual_funds = combined["fund_ticker"].nunique()
    if actual_funds != expected_funds:
        print(
            f"ERROR: Combined-output fund count mismatch: "
            f"expected {expected_funds}, found {actual_funds}.",
            file=sys.stderr,
        )
        return 1

    output_dir: Path = args.out_dir
    combined_path = output_dir / args.combined_name

    try:
        if not args.no_per_fund:
            for ticker in tickers:
                if ticker not in successful:
                    continue
                per_fund_path = output_dir / f"{ticker}_holdings_latest.csv"
                write_csv_atomic(successful[ticker], per_fund_path)

        write_csv_atomic(combined, combined_path)
    except Exception as exc:
        print(f"ERROR: Could not write output CSV: {exc}", file=sys.stderr)
        return 1

    print(
        f"\nCombined stream: {len(combined)} rows across {actual_funds} funds "
        f"-> {combined_path}",
        file=sys.stderr,
    )
    print("Sources used:", file=sys.stderr)
    for ticker in tickers:
        if ticker in sources_used:
            print(f"  - {ticker}: {sources_used[ticker]}", file=sys.stderr)

    if failures:
        print(
            "WARNING: Partial output was published because --allow-partial was set. "
            f"Failed funds: {', '.join(failures)}",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
