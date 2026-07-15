#!/usr/bin/env python3
"""
GitHub-Actions-friendly ETF holdings puller using Perplexity Finance's
undocumented holdings endpoint.

The script downloads holdings for every requested US-listed ETF, normalises
them into one stable CSV schema, validates that every requested fund succeeded,
and only then atomically replaces the live output file. This prevents a failed
or partial weekly run from silently replacing a valid CSV used by Excel.

Default output:
    data/ETF_Holdings_Latest.csv

Examples:
    python scrape_etf_holdings_updated.py VGT ACWI XLF

    python scrape_etf_holdings_updated.py \
        --no-per-fund \
        VGT ACWI XLF XLI XLC PPH MLPX GRID SOXQ

    python scrape_etf_holdings_updated.py \
        --out-dir data \
        --combined-name ETF_Holdings_Latest.csv \
        --no-per-fund \
        VGT ACWI

Requires:
    pip install requests pandas

Important:
    This relies on an undocumented endpoint behind Perplexity's finance UI:
        https://www.perplexity.ai/rest/finance/holdings/<TICKER>

    It is not a published or guaranteed API and may change or be disabled.
    The source_date field is the date this script retrieved the data; it is
    not represented as the issuer's official holdings effective date.
"""

from __future__ import annotations

import argparse
import os
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

# The first five fields retain compatibility with the Power Query schema
# previously proposed for the Excel workbook. Extra fields remain available
# for audit and analysis.
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
            "Accept": "application/json, text/plain, */*",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def extract_holdings_payload(payload: Any, ticker: str) -> list[dict[str, Any]]:
    """Return a list of holdings from the currently observed API payload.

    The endpoint currently returns a bare list. A small number of common
    wrapper keys are also supported so a minor response-format change does not
    immediately break the pipeline.
    """
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
                f"Unexpected JSON structure returned for {ticker}; "
                f"top-level keys: {sorted(payload.keys())[:10]}"
            )
    else:
        raise HoldingsError(
            f"Unexpected JSON type returned for {ticker}: "
            f"{type(payload).__name__}"
        )

    if not records:
        raise HoldingsError(f"No holdings returned for {ticker}")

    if not all(isinstance(item, dict) for item in records):
        raise HoldingsError(f"Malformed holdings records returned for {ticker}")

    return records


def fetch_perplexity(
    session: requests.Session,
    ticker: str,
    timeout_seconds: int,
) -> pd.DataFrame:
    """Fetch and minimally standardise a fund's holdings response."""
    ticker = ticker.strip().upper()
    url = f"https://www.perplexity.ai/rest/finance/holdings/{ticker}"

    response = session.get(url, timeout=(10, timeout_seconds))
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        preview = response.text[:200].replace("\n", " ")
        raise HoldingsError(
            f"Non-JSON response returned for {ticker}: {preview!r}"
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
        raise HoldingsError(f"Required 'weight' field missing for {ticker}")

    for column in ("holding_ticker", "holding_name", "shares_held", "market_value_usd"):
        if column not in frame.columns:
            frame[column] = pd.NA

    frame["weight"] = pd.to_numeric(frame["weight"], errors="coerce")
    frame["shares_held"] = pd.to_numeric(frame["shares_held"], errors="coerce")
    frame["market_value_usd"] = pd.to_numeric(
        frame["market_value_usd"], errors="coerce"
    )

    if frame["weight"].notna().sum() == 0:
        raise HoldingsError(f"No numeric holding weights returned for {ticker}")

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
    failures: dict[str, str] = {}

    with build_session() as session:
        for ticker in tickers:
            print(f"[{ticker}] Fetching holdings...", file=sys.stderr)
            try:
                raw = fetch_perplexity(session, ticker, args.timeout)
                normalised = normalize(raw, ticker, retrieved_at)
                validate_normalized(normalised, ticker, args.min_rows)
                successful[ticker] = normalised

                top_weight = normalised["weight"].dropna().iloc[0]
                print(
                    f"[{ticker}] OK: {len(normalised)} rows "
                    f"(top holding weight {top_weight:.4g})",
                    file=sys.stderr,
                )
            except Exception as exc:  # one failed ticker is reported with context
                failures[ticker] = str(exc)
                print(f"[{ticker}] FAILED: {exc}", file=sys.stderr)

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

    if failures:
        print(
            "WARNING: Partial output was published because --allow-partial was set. "
            f"Failed funds: {', '.join(failures)}",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())