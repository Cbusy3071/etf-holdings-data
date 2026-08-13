#!/usr/bin/env python3
"""
Provider-first ETF holdings pipeline.

Primary sources:
  VGT   Vanguard snapshot + Yahoo prices
  ACWI  iShares daily holdings CSV
  XLF   State Street daily holdings XLSX
  XLI   State Street daily holdings XLSX
  XLC   State Street daily holdings XLSX
  PPH   VanEck official holdings XLSX
  MLPX  Global X official holdings CSV
  GRID  First Trust official holdings page
  SOXQ  Invesco official export, with safe last-known-good fallback

Output:
  data/ETF_Holdings_Latest.csv

Exact output schema:
  source_date
  retrieved_at_utc
  fund_ticker
  provider
  rank
  holding_ticker
  holding_name
  weight
  shares_held
  market_value_usd

Dependencies:
  pip install pandas requests yfinance openpyxl lxml html5lib
"""

from __future__ import annotations

import argparse
import html as html_lib
import io
import json
import math
import os
import re
import sys
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


# ============================================================
# CONFIG
# ============================================================

DEFAULT_OUT_DIR = Path("data")
DEFAULT_COMBINED_NAME = "ETF_Holdings_Latest.csv"
DEFAULT_TIMEOUT_SECONDS = 30

DEFAULT_ETFS = [
    "VGT",
    "ACWI",
    "XLF",
    "XLI",
    "XLC",
    "PPH",
    "MLPX",
    "GRID",
    "SOXQ",
]

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

MIN_PREVIOUS_COUNT_RATIO = 0.90

MIN_TOTAL_WEIGHT = 98.0
MAX_TOTAL_WEIGHT = 102.0

VGT_MAX_TOTAL_FALLBACK_WEIGHT_PCT = 0.05

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


# ============================================================
# PROVIDER URLS
# ============================================================

SSGA_URL = (
    "https://www.ssga.com/us/en/intermediary/etfs/"
    "library-content/products/fund-data/etfs/us/"
    "holdings-daily-us-en-{ticker}.xlsx"
)

ISHARES_ACWI_URL = (
    "https://www.ishares.com/us/products/239600/"
    "ishares-msci-acwi-etf/latest-holdings.csv"
)

VANECK_PAGE_URL = (
    "https://www.vaneck.com/us/en/investments/"
    "pharmaceutical-etf-pph/holdings/"
)

VANECK_HOLDINGS_XLSX_URL = (
    "https://www.vaneck.com/us/en/investments/"
    "pharmaceutical-etf-pph/downloads/holdings/"
)

GLOBALX_PAGE_URL = (
    "https://www.globalxetfs.com/funds/{ticker}"
)

FIRSTTRUST_URL = (
    "https://www.ftportfolios.com/Retail/Etf/"
    "EtfHoldings.aspx?Ticker={ticker}"
)

INVESCO_PAGE_URL = (
    "https://www.invesco.com/us/en/financial-products/"
    "etfs/invesco-phlx-semiconductor-etf.html"
)

INVESCO_EXPORT_URL = (
    "https://www.invesco.com/us/financial-products/"
    "etfs/holdings/main/holdings/0"
    "?audienceType=Investor"
    "&action=download"
    "&ticker={ticker}"
)

VANGUARD_URL = (
    "https://investor.vanguard.com/"
    "investment-products/etfs/profile/api/"
    "{ticker}/portfolio-holding/stock"
)


# ============================================================
# ERRORS / TYPES
# ============================================================

class HoldingsError(RuntimeError):
    pass


@dataclass
class FetchResult:
    frame: pd.DataFrame
    source_date: str
    precision_method: str


# ============================================================
# TIME
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_today() -> str:
    return utc_now().date().isoformat()


# ============================================================
# HTTP
# ============================================================

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


def http_get(
    session: requests.Session,
    url: str,
    timeout: int,
    *,
    headers: dict[str, str] | None = None,
) -> requests.Response:

    request_headers = HEADERS.copy()

    if headers:
        request_headers.update(headers)

    response = session.get(
        url,
        headers=request_headers,
        timeout=(10, timeout),
        allow_redirects=True,
    )

    if response.status_code != 200:
        raise HoldingsError(
            f"HTTP {response.status_code}: {url}"
        )

    if len(response.content) < 100:
        raise HoldingsError(
            f"Suspiciously small response "
            f"({len(response.content)} bytes): {url}"
        )

    return response


# ============================================================
# GENERIC CLEANING
# ============================================================

def clean_text(value: Any) -> str:
    """
    Convert provider text to plain clean text.

    Old fallback rows may contain:
      <span class="truncated_text_single"
            title="Novo Nordisk A/S">
            Novo Nordisk A/..
      </span>

    Extract title before stripping tags.
    """

    if value is None or value is pd.NA:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    text = str(value).strip()

    if text.lower() in {
        "",
        "nan",
        "none",
        "null",
    }:
        return ""

    text = html_lib.unescape(text)

    title_match = re.search(
        r"""title\s*=\s*["']([^"']+)["']""",
        text,
        flags=re.IGNORECASE,
    )

    if title_match:
        text = title_match.group(1)
    else:
        text = re.sub(
            r"<[^>]+>",
            " ",
            text,
        )

    text = html_lib.unescape(text)

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    return text.strip()


def to_float(value: Any) -> float | None:

    if value is None or value is pd.NA:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = str(value).strip()

    if not text:
        return None

    if text.upper() in {
        "-",
        "--",
        "N/A",
        "NA",
        "NONE",
        "NAN",
        "NULL",
    }:
        return None

    negative = (
        text.startswith("(")
        and text.endswith(")")
    )

    text = (
        text
        .replace(",", "")
        .replace("%", "")
        .replace("$", "")
        .strip("() ")
    )

    text = re.sub(
        r"[^0-9eE+\-.]",
        "",
        text,
    )

    if not text:
        return None

    try:
        number = float(text)

        if negative:
            number = -number

        return number

    except ValueError:
        return None


def normalize_date_value(
    value: Any,
) -> str | None:

    if value is None:
        return None

    if isinstance(
        value,
        (datetime, pd.Timestamp),
    ):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    text = clean_text(value)

    if not text:
        return None

    patterns = [
        (
            r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b",
            "ymd",
        ),
        (
            r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b",
            "mdy",
        ),
    ]

    for pattern, style in patterns:

        match = re.search(
            pattern,
            text,
        )

        if not match:
            continue

        try:
            if style == "ymd":
                year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))

            else:
                month = int(match.group(1))
                day = int(match.group(2))
                year = int(match.group(3))

            return date(
                year,
                month,
                day,
            ).isoformat()

        except ValueError:
            pass

    for fmt in (
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d %Y",
        "%b %d %Y",
    ):

        try:
            return (
                datetime
                .strptime(text, fmt)
                .date()
                .isoformat()
            )

        except ValueError:
            pass

    return None


def extract_date_from_text(
    text: str,
) -> str | None:

    patterns = [
        (
            r"(?i)as\s+of[^0-9A-Za-z]{0,20}"
            r"([A-Za-z]+\s+\d{1,2},?\s+20\d{2})"
        ),
        (
            r"(?i)as\s+of[^0-9]{0,20}"
            r"(\d{1,2}/\d{1,2}/20\d{2})"
        ),
        (
            r"(?i)as\s+of[^0-9]{0,20}"
            r"(20\d{2}-\d{1,2}-\d{1,2})"
        ),
    ]

    for pattern in patterns:

        match = re.search(
            pattern,
            text,
        )

        if not match:
            continue

        parsed = normalize_date_value(
            match.group(1)
        )

        if parsed:
            return parsed

    candidates = re.findall(
        r"\b(?:"
        r"20\d{2}-\d{1,2}-\d{1,2}"
        r"|"
        r"\d{1,2}/\d{1,2}/20\d{2}"
        r"|"
        r"[A-Za-z]+\s+\d{1,2},?\s+20\d{2}"
        r")\b",
        text[:20000],
    )

    for candidate in candidates:

        parsed = normalize_date_value(
            candidate
        )

        if parsed:
            return parsed

    return None


def pick_col(
    df: pd.DataFrame,
    candidates: list[str],
) -> str | None:

    columns = [
        (
            str(column),
            clean_text(column).lower(),
        )
        for column in df.columns
    ]

    for candidate in candidates:

        candidate_lower = candidate.lower()

        for original, lowered in columns:

            if lowered == candidate_lower:
                return original

    for candidate in candidates:

        candidate_lower = candidate.lower()

        for original, lowered in columns:

            if candidate_lower in lowered:
                return original

    return None


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

    output = pd.DataFrame(
        index=df.index
    )

    if ticker and ticker in df.columns:
        output["raw_ticker"] = (
            df[ticker]
            .map(clean_text)
        )
    else:
        output["raw_ticker"] = ""

    if name and name in df.columns:
        output["raw_name"] = (
            df[name]
            .map(clean_text)
        )
    else:
        output["raw_name"] = ""

    if weight and weight in df.columns:
        output["weight"] = (
            df[weight]
            .map(to_float)
        )
    else:
        output["weight"] = pd.NA

    if shares and shares in df.columns:
        output["shares"] = (
            df[shares]
            .map(to_float)
        )
    else:
        output["shares"] = pd.NA

    if mval and mval in df.columns:
        output["market_value"] = (
            df[mval]
            .map(to_float)
        )
    else:
        output["market_value"] = pd.NA

    if isin and isin in df.columns:
        output["isin"] = (
            df[isin]
            .map(clean_text)
        )
    else:
        output["isin"] = ""

    if sedol and sedol in df.columns:
        output["sedol"] = (
            df[sedol]
            .map(clean_text)
        )
    else:
        output["sedol"] = ""

    output = output[
        (
            output["raw_ticker"].ne("")
            |
            output["raw_name"].ne("")
        )
    ].copy()

    output["raw_ticker"] = (
        output["raw_ticker"]
        .astype(str)
        .str.strip()
    )

    output["raw_name"] = (
        output["raw_name"]
        .astype(str)
        .str.strip()
    )

    output["isin"] = (
        output["isin"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    output["sedol"] = (
        output["sedol"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    return (
        output[
            STD_COLS
        ]
        .reset_index(drop=True)
    )


# ============================================================
# NON-SECURITY FILTER
# ============================================================

def is_nonsecurity(
    ticker: str,
    name: str,
) -> bool:

    ticker_upper = (
        clean_text(ticker)
        .upper()
        .strip()
    )

    name_upper = (
        clean_text(name)
        .upper()
        .strip()
    )

    if not ticker_upper:
        return True

    if ticker_upper in {
        "TICKER",
        "SYMBOL",
        "N/A",
        "NA",
        "NONE",
        "--",
        "USD",
        "EUR",
        "GBP",
        "CHF",
        "CAD",
        "AUD",
        "JPY",
        "HKD",
        "CNY",
        "TWD",
        "KRW",
        "AGPXX",
        "BNYMLEND",
    }:
        return True

    if ticker_upper.startswith("$"):
        return True

    if "CASH" in ticker_upper:
        return True

    if name_upper in {
        "US DOLLAR",
        "USD CASH",
        "EURO",
        "POUND STERLING",
        "SWISS FRANC",
        "HONG KONG DOLLAR",
        "YUAN RENMINBI",
        "BRAZILIAN REAL",
        "NEW TAIWAN DOLLAR",
        "OTHER/CASH",
        "CASH",
    }:
        return True

    if "SECURITIES LENDING" in name_upper:
        return True

    if (
        "GOVERNMENT & AGENCY PORTFOLIO"
        in name_upper
    ):
        return True

    return False


def drop_nonsecurity_rows(
    df: pd.DataFrame,
) -> pd.DataFrame:

    output = df.copy()

    output["raw_ticker"] = (
        output["raw_ticker"]
        .map(clean_text)
    )

    output["raw_name"] = (
        output["raw_name"]
        .map(clean_text)
    )

    keep = [
        not is_nonsecurity(
            ticker,
            name,
        )
        for ticker, name in zip(
            output["raw_ticker"],
            output["raw_name"],
        )
    ]

    return (
        output.loc[keep]
        .reset_index(drop=True)
    )


# ============================================================
# WEIGHTS
# ============================================================

def maybe_recompute_from_market_value(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """
    Recompute full-precision weights when complete market values exist.

    Negative market values are allowed only for genuine
    cash / FX / financing rows. This is important for ACWI,
    where iShares can publish a negative USD cash balance.

    The signed portfolio market value is used as the denominator.
    """

    output = df.copy()

    if output.empty:
        return output, "provider_weight"

    market_value = pd.to_numeric(
        output["market_value"],
        errors="coerce",
    )

    # Require complete market-value coverage.
    if not market_value.notna().all():
        return output, "provider_weight"

    # Negative values are permitted only for non-security rows
    # such as cash / FX.
    negative_mask = market_value < 0

    if negative_mask.any():

        for idx in output.index[negative_mask]:

            ticker = clean_text(
                output.at[
                    idx,
                    "raw_ticker",
                ]
            )

            name = clean_text(
                output.at[
                    idx,
                    "raw_name",
                ]
            )

            if not is_nonsecurity(
                ticker,
                name,
            ):
                return (
                    output,
                    "provider_weight",
                )

    # Signed denominator:
    # positive securities + positive cash - negative cash.
    total = float(
        market_value.sum()
    )

    if (
        not math.isfinite(total)
        or total <= 0
    ):
        return (
            output,
            "provider_weight",
        )

    output["weight"] = (
        market_value
        / total
        * 100.0
    )

    return (
        output,
        "market_value_recomputed",
    )


# ============================================================
# CSV / EXCEL PARSING
# ============================================================

def sniff_csv(
    text: str,
    must_contain: list[str],
) -> pd.DataFrame:

    lines = text.splitlines()

    header_index = None

    for index, line in enumerate(
        lines[:100]
    ):

        lowered = line.lower()

        if all(
            token.lower() in lowered
            for token in must_contain
        ):
            header_index = index
            break

    if header_index is None:
        raise HoldingsError(
            "Could not locate CSV header "
            f"containing {must_contain}. "
            f"First lines: {lines[:5]}"
        )

    frame = pd.read_csv(
        io.StringIO(
            "\n".join(
                lines[header_index:]
            )
        ),
        dtype=str,
    )

    frame.columns = [
        clean_text(column)
        for column in frame.columns
    ]

    return frame


def excel_with_detected_header(
    content: bytes,
    *,
    required_tokens: tuple[str, ...],
    max_scan_rows: int = 50,
) -> tuple[
    pd.DataFrame,
    str | None,
]:

    raw = pd.read_excel(
        io.BytesIO(content),
        header=None,
        dtype=str,
    )

    header_index = None

    for index in range(
        min(
            max_scan_rows,
            len(raw),
        )
    ):

        row_text = " | ".join(
            clean_text(value).lower()
            for value
            in raw.iloc[index].tolist()
        )

        if all(
            token.lower() in row_text
            for token
            in required_tokens
        ):
            header_index = index
            break

    if header_index is None:
        raise HoldingsError(
            "Could not locate Excel holdings header "
            f"containing {required_tokens}"
        )

    source_date = None

    for index in range(header_index):

        row_text = " ".join(
            clean_text(value)
            for value
            in raw.iloc[index].tolist()
        )

        parsed = extract_date_from_text(
            row_text
        )

        if parsed:
            source_date = parsed
            break

    frame = (
        raw
        .iloc[
            header_index + 1 :
        ]
        .copy()
    )

    frame.columns = [
        clean_text(column)
        for column
        in raw.iloc[
            header_index
        ].tolist()
    ]

    return (
        frame,
        source_date,
    )


def parse_downloaded_holdings(
    response: requests.Response,
    *,
    provider: str,
    source_date_hint: str | None = None,
) -> tuple[
    pd.DataFrame,
    str | None,
]:

    content_type = (
        response
        .headers
        .get(
            "content-type",
            "",
        )
        .lower()
    )

    final_url = (
        str(response.url)
        .lower()
    )

    is_excel = (
        "spreadsheet" in content_type
        or
        "excel" in content_type
        or
        final_url.endswith(
            (
                ".xls",
                ".xlsx",
            )
        )
        or
        response.content[:2]
        == b"PK"
    )

    if is_excel:

        frame, source_date = (
            excel_with_detected_header(
                response.content,
                required_tokens=(
                    "ticker",
                ),
            )
        )

        return (
            frame,
            source_date
            or source_date_hint,
        )

    text = response.text

    if (
        "ticker"
        in text[:10000].lower()
        and
        ","
        in text[:3000]
    ):

        frame = sniff_csv(
            text,
            ["ticker"],
        )

        source_date = (
            extract_date_from_text(
                text[:20000]
            )
            or source_date_hint
        )

        return (
            frame,
            source_date,
        )

    raise HoldingsError(
        f"{provider}: response was not "
        "recognisable CSV or Excel "
        f"(content-type={content_type}, "
        f"url={response.url})"
    )


# ============================================================
# DOWNLOAD LINK DISCOVERY
# ============================================================

def find_download_link(
    html_text: str,
    base_url: str,
    *,
    text_contains: tuple[str, ...] = (),
    href_contains: tuple[str, ...] = (),
) -> str | None:

    try:
        root = lxml_html.fromstring(
            html_text
        )

    except Exception:
        return None

    text_tokens = tuple(
        token.lower()
        for token
        in text_contains
    )

    href_tokens = tuple(
        token.lower()
        for token
        in href_contains
    )

    matches: list[
        tuple[
            int,
            str,
        ]
    ] = []

    for anchor in root.xpath(
        "//a[@href]"
    ):

        href = clean_text(
            anchor.get("href")
        )

        if not href:
            continue

        if href.lower().startswith(
            (
                "javascript:",
                "mailto:",
                "#",
            )
        ):
            continue

        label = " ".join(
            clean_text(part)
            for part
            in anchor.itertext()
        ).lower()

        href_lower = (
            href.lower()
        )

        if text_tokens:

            if not all(
                token in label
                for token
                in text_tokens
            ):
                continue

        if href_tokens:

            if not all(
                token in href_lower
                for token
                in href_tokens
            ):
                continue

        score = 0

        if "download" in label:
            score += 3

        if "full holdings" in label:
            score += 3

        if href_lower.endswith(
            (
                ".csv",
                ".xls",
                ".xlsx",
            )
        ):
            score += 3

        if "holding" in href_lower:
            score += 1

        matches.append(
            (
                score,
                urljoin(
                    base_url,
                    href,
                ),
            )
        )

    if not matches:
        return None

    matches.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    return matches[0][1]


# ============================================================
# YAHOO SYMBOL NORMALISATION
# ============================================================

YAHOO_EXCHANGE_SUFFIXES = {
    "TW",
    "KS",
    "KQ",
    "L",
    "PA",
    "AS",
    "DE",
    "SW",
    "TO",
    "HK",
    "T",
    "AX",
    "MC",
    "MI",
    "HE",
    "ST",
    "CO",
    "OL",
    "BR",
    "LS",
    "SI",
    "NZ",
    "TA",
}


def yahoo_ticker(
    raw_ticker: str,
) -> str:

    ticker = (
        clean_text(raw_ticker)
        .upper()
        .strip()
    )

    ticker = re.sub(
        r"\s+(US|UN|UW|UQ|UR|UF)$",
        "",
        ticker,
    )

    match = re.fullmatch(
        r"[A-Z0-9]+\.([A-Z]{1,4})",
        ticker,
    )

    if match:

        suffix = match.group(1)

        if suffix in YAHOO_EXCHANGE_SUFFIXES:
            return ticker

    ticker = (
        ticker
        .replace("/", "-")
        .replace(".", "-")
    )

    return ticker


# ============================================================
# YAHOO PRICES
# ============================================================

def _field_frame(
    data: pd.DataFrame,
    field: str,
    tickers: list[str],
) -> pd.DataFrame:

    if isinstance(
        data.columns,
        pd.MultiIndex,
    ):

        first_level = (
            data.columns
            .get_level_values(0)
        )

        if field not in first_level:
            return pd.DataFrame(
                index=data.index
            )

        frame = data[field]

        if isinstance(
            frame,
            pd.Series,
        ):

            name = (
                tickers[0]
                if tickers
                else field
            )

            frame = frame.to_frame(
                name
            )

        return frame

    if field not in data.columns:
        return pd.DataFrame(
            index=data.index
        )

    series = data[field]

    if isinstance(
        series,
        pd.Series,
    ):

        name = (
            tickers[0]
            if len(tickers) == 1
            else field
        )

        return series.to_frame(
            name
        )

    return series


def yahoo_latest_prices(
    tickers: Iterable[str],
) -> dict[str, float]:

    import yfinance as yf

    raw_to_yahoo: dict[
        str,
        str,
    ] = {}

    for raw in tickers:

        raw_clean = clean_text(
            raw
        )

        if not raw_clean:
            continue

        converted = yahoo_ticker(
            raw_clean
        )

        if converted:
            raw_to_yahoo[
                raw_clean
            ] = converted

    yahoo_symbols = sorted(
        set(
            raw_to_yahoo.values()
        )
    )

    if not yahoo_symbols:
        return {}

    try:

        data = yf.download(
            tickers=yahoo_symbols,
            period="5d",
            interval="1d",
            auto_adjust=False,
            keepna=False,
            progress=False,
            threads=True,
            timeout=15,
            group_by="column",
        )

    except Exception as exc:

        raise HoldingsError(
            "Yahoo latest-price download failed: "
            f"{exc}"
        ) from exc

    if data is None or len(data) == 0:

        raise HoldingsError(
            "Yahoo returned no price history"
        )

    closes = _field_frame(
        data,
        "Close",
        yahoo_symbols,
    )

    yahoo_prices: dict[
        str,
        float,
    ] = {}

    for symbol in yahoo_symbols:

        if symbol not in closes.columns:
            continue

        series = pd.to_numeric(
            closes[symbol],
            errors="coerce",
        ).dropna()

        if series.empty:
            continue

        price = float(
            series.iloc[-1]
        )

        if price > 0:
            yahoo_prices[
                symbol
            ] = price

    result: dict[
        str,
        float,
    ] = {}

    for raw, symbol in (
        raw_to_yahoo.items()
    ):

        if symbol in yahoo_prices:

            result[
                raw
            ] = yahoo_prices[
                symbol
            ]

    return result


def fill_std_market_values_from_yahoo(
    frame: pd.DataFrame,
    *,
    label: str,
) -> pd.DataFrame:

    output = frame.copy()

    market_value = pd.to_numeric(
        output["market_value"],
        errors="coerce",
    )

    shares = pd.to_numeric(
        output["shares"],
        errors="coerce",
    )

    need = (
        market_value.isna()
        &
        shares.notna()
        &
        (shares > 0)
        &
        output[
            "raw_ticker"
        ]
        .astype(str)
        .ne("")
    )

    if not need.any():
        return output

    required_tickers = (
        output.loc[
            need,
            "raw_ticker",
        ]
        .map(clean_text)
    )

    prices = yahoo_latest_prices(
        required_tickers
    )

    filled = 0

    for index in (
        output.index[need]
    ):

        ticker = clean_text(
            output.at[
                index,
                "raw_ticker",
            ]
        )

        share_count = to_float(
            output.at[
                index,
                "shares",
            ]
        )

        price = prices.get(
            ticker
        )

        if (
            price is None
            or
            share_count is None
            or
            share_count <= 0
        ):
            continue

        output.at[
            index,
            "market_value",
        ] = (
            share_count
            * price
        )

        filled += 1

    remaining = int(
        pd.to_numeric(
            output[
                "market_value"
            ],
            errors="coerce",
        )
        .isna()
        .sum()
    )

    print(
        f"[{label}] Yahoo MV backfill: "
        f"{filled} filled, "
        f"{remaining} blank",
        file=sys.stderr,
    )

    return output


# ============================================================
# ISHARES TICKER QUALIFICATION
# ============================================================

ISHARES_EXCHANGE_SUFFIX = {
    "Taiwan Stock Exchange": ".TW",
    "Korea Exchange (Stock Market)": ".KS",
    "Kosdaq Market": ".KQ",
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


def qualify_ishares_ticker(
    raw_ticker: Any,
    exchange: Any,
) -> str:

    ticker = clean_text(
        raw_ticker
    )

    exchange_clean = clean_text(
        exchange
    )

    if not ticker:
        return ""

    suffix = (
        ISHARES_EXCHANGE_SUFFIX
        .get(
            exchange_clean
        )
    )

    if not suffix:
        return ticker

    if "." in ticker:
        return ticker

    return (
        ticker
        + suffix
    )


# ============================================================
# STATE STREET
# ============================================================

def fetch_ssga(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    url = SSGA_URL.format(
        ticker=etf.lower()
    )

    response = http_get(
        session,
        url,
        timeout,
    )

    raw = pd.read_excel(
        io.BytesIO(
            response.content
        ),
        header=None,
        dtype=str,
    )

    header_index = None

    for index in range(
        min(
            30,
            len(raw),
        )
    ):

        row_text = " ".join(
            clean_text(value).lower()
            for value
            in raw.iloc[
                index
            ].tolist()
        )

        if (
            "ticker"
            in row_text
            and
            "weight"
            in row_text
        ):
            header_index = index
            break

    if header_index is None:

        raise HoldingsError(
            f"{etf}: State Street holdings "
            "header not found"
        )

    source_date = None

    for index in range(
        header_index
    ):

        row_text = " ".join(
            clean_text(value)
            for value
            in raw.iloc[
                index
            ].tolist()
        )

        parsed = extract_date_from_text(
            row_text
        )

        if parsed:
            source_date = parsed
            break

    frame = (
        raw
        .iloc[
            header_index + 1 :
        ]
        .copy()
    )

    frame.columns = [
        clean_text(column)
        for column
        in raw.iloc[
            header_index
        ].tolist()
    ]

    result = standardise(
        frame,
        ticker=pick_col(
            frame,
            ["ticker"],
        ),
        name=pick_col(
            frame,
            [
                "name",
                "security",
            ],
        ),
        weight=pick_col(
            frame,
            ["weight"],
        ),
        shares=pick_col(
            frame,
            [
                "shares held",
                "shares",
            ],
        ),
        mval=pick_col(
            frame,
            [
                "market value",
            ],
        ),
        isin=pick_col(
            frame,
            ["isin"],
        ),
        sedol=pick_col(
            frame,
            ["sedol"],
        ),
    )

    result = drop_nonsecurity_rows(
        result
    )

    result = (
        fill_std_market_values_from_yahoo(
            result,
            label=etf,
        )
    )

    return FetchResult(
        result,
        source_date or iso_today(),
        "provider_weight",
    )


# ============================================================
# ISHARES / ACWI
# ============================================================

def fetch_ishares(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    response = http_get(
        session,
        ISHARES_ACWI_URL,
        timeout,
    )

    text = response.text

    frame = sniff_csv(
        text,
        [
            "ticker",
            "name",
            "weight",
        ],
    )

    ticker_column = pick_col(
        frame,
        ["ticker"],
    )

    exchange_column = pick_col(
        frame,
        ["exchange"],
    )

    if (
        ticker_column
        and
        exchange_column
    ):

        frame = frame.copy()

        frame[
            ticker_column
        ] = [
            qualify_ishares_ticker(
                ticker,
                exchange,
            )
            for ticker, exchange
            in zip(
                frame[
                    ticker_column
                ],
                frame[
                    exchange_column
                ],
            )
        ]

    result_all = standardise(
        frame,
        ticker=ticker_column,
        name=pick_col(
            frame,
            ["name"],
        ),
        weight=pick_col(
            frame,
            ["weight"],
        ),
        shares=pick_col(
            frame,
            [
                "quantity",
                "shares",
            ],
        ),
        mval=pick_col(
            frame,
            [
                "market value",
            ],
        ),
        isin=pick_col(
            frame,
            ["isin"],
        ),
        sedol=pick_col(
            frame,
            ["sedol"],
        ),
    )

    # Recompute BEFORE removing cash/FX.
    #
    # This preserves the complete signed fund denominator,
    # including any negative USD cash balance.
    result_all, precision = (
        maybe_recompute_from_market_value(
            result_all
        )
    )

    # CRITICAL ACWI PRECISION GUARD:
    #
    # Never silently fall back to iShares' rounded 2dp
    # provider weights when complete market-value data should
    # allow full-precision reconstruction.
    if precision != "market_value_recomputed":
        raise HoldingsError(
            "ACWI: iShares market-value recomputation did not occur; "
            "refusing to publish rounded provider weights"
        )

    result = drop_nonsecurity_rows(
        result_all
    )

    source_date = (
        extract_date_from_text(
            text[:30000]
        )
        or iso_today()
    )

    return FetchResult(
        result,
        source_date,
        precision,
    )


# ============================================================
# GLOBAL X
# ============================================================

def fetch_globalx(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    page_url = (
        GLOBALX_PAGE_URL
        .format(
            ticker=etf.upper()
        )
    )

    page = http_get(
        session,
        page_url,
        timeout,
    )

    source_date = (
        extract_date_from_text(
            page.text[:50000]
        )
        or iso_today()
    )

    csv_url = (
        find_download_link(
            page.text,
            page.url,
            text_contains=(
                "full holdings",
            ),
            href_contains=(
                ".csv",
            ),
        )
    )

    if not csv_url:

        try:

            root = lxml_html.fromstring(
                page.text
            )

            candidates: list[
                tuple[
                    int,
                    str,
                ]
            ] = []

            for anchor in root.xpath(
                "//a[@href]"
            ):

                href = clean_text(
                    anchor.get(
                        "href"
                    )
                )

                lowered = (
                    href.lower()
                )

                if ".csv" not in lowered:
                    continue

                score = 0

                if (
                    etf.lower()
                    in lowered
                ):
                    score += 3

                if (
                    "holding"
                    in lowered
                ):
                    score += 2

                candidates.append(
                    (
                        score,
                        urljoin(
                            page.url,
                            href,
                        ),
                    )
                )

            if candidates:

                candidates.sort(
                    key=lambda item: item[0],
                    reverse=True,
                )

                csv_url = (
                    candidates[0][1]
                )

        except Exception:
            pass

    if not csv_url:

        raise HoldingsError(
            f"{etf}: Global X "
            "Full Holdings CSV "
            "link not found"
        )

    response = http_get(
        session,
        csv_url,
        timeout,
    )

    frame, download_date = (
        parse_downloaded_holdings(
            response,
            provider="Global X",
            source_date_hint=source_date,
        )
    )

    result_all = standardise(
        frame,
        ticker=pick_col(
            frame,
            ["ticker"],
        ),
        name=pick_col(
            frame,
            ["name"],
        ),
        weight=pick_col(
            frame,
            [
                "% of net assets",
                "net assets",
                "weight",
            ],
        ),
        shares=pick_col(
            frame,
            [
                "shares held",
                "shares",
                "quantity",
            ],
        ),
        mval=pick_col(
            frame,
            ["market value"],
        ),
        isin=pick_col(
            frame,
            ["isin"],
        ),
        sedol=pick_col(
            frame,
            ["sedol"],
        ),
    )

    result_all, precision = (
        maybe_recompute_from_market_value(
            result_all
        )
    )

    result = drop_nonsecurity_rows(
        result_all
    )

    return FetchResult(
        result,
        download_date
        or source_date,
        precision,
    )


# ============================================================
# FIRST TRUST / GRID
# ============================================================

def parse_firsttrust_dom(
    html_text: str,
) -> pd.DataFrame:

    try:
        root = lxml_html.fromstring(
            html_text
        )

    except Exception as exc:

        raise HoldingsError(
            "First Trust HTML parse failed: "
            f"{exc}"
        ) from exc

    records: list[
        dict[str, Any]
    ] = []

    for row in root.xpath(
        "//tr"
    ):

        cells = [
            " ".join(
                clean_text(part)
                for part
                in cell.itertext()
            ).strip()
            for cell
            in row.xpath(
                "./th|./td"
            )
        ]

        cells = [
            cell
            for cell
            in cells
            if cell != ""
        ]

        if len(cells) < 6:
            continue

        weight = to_float(
            cells[-1]
        )

        market_value = to_float(
            cells[-2]
        )

        shares = to_float(
            cells[-3]
        )

        if (
            weight is None
            or
            market_value is None
            or
            shares is None
        ):
            continue

        ticker = clean_text(
            cells[1]
        )

        name = clean_text(
            cells[0]
        )

        if not ticker:
            continue

        if ticker.lower() in {
            "identifier",
            "ticker",
        }:
            continue

        records.append(
            {
                "raw_ticker": ticker,
                "raw_name": name,
                "weight": weight,
                "shares": shares,
                "market_value": (
                    market_value
                ),
                "isin": "",
                "sedol": "",
            }
        )

    if not records:

        raise HoldingsError(
            "GRID: no First Trust "
            "holdings rows found"
        )

    return pd.DataFrame(
        records,
        columns=STD_COLS,
    )


def fetch_firsttrust(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    url = FIRSTTRUST_URL.format(
        ticker=etf
    )

    response = http_get(
        session,
        url,
        timeout,
    )

    source_date = (
        extract_date_from_text(
            response.text[:50000]
        )
        or iso_today()
    )

    result_all = (
        parse_firsttrust_dom(
            response.text
        )
    )

    result_all, precision = (
        maybe_recompute_from_market_value(
            result_all
        )
    )

    result = drop_nonsecurity_rows(
        result_all
    )

    return FetchResult(
        result,
        source_date,
        precision,
    )


# ============================================================
# VANECK / PPH
# ============================================================

def fetch_vaneck(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    errors: list[str] = []

    source_date = iso_today()

    try:

        response = http_get(
            session,
            VANECK_HOLDINGS_XLSX_URL,
            timeout,
            headers={
                "Referer": VANECK_PAGE_URL,
                "Accept": (
                    "application/vnd.openxmlformats-"
                    "officedocument.spreadsheetml.sheet,"
                    "application/vnd.ms-excel,"
                    "application/octet-stream;q=0.9,"
                    "*/*;q=0.8"
                ),
            },
        )

        frame, download_date = (
            parse_downloaded_holdings(
                response,
                provider="VanEck",
                source_date_hint=(
                    source_date
                ),
            )
        )

        result_all = standardise(
            frame,
            ticker=pick_col(
                frame,
                [
                    "ticker",
                    "symbol",
                ],
            ),
            name=pick_col(
                frame,
                [
                    "holding name",
                    "name",
                    "holding",
                    "security",
                ],
            ),
            weight=pick_col(
                frame,
                [
                    "% of net assets",
                    "% of net",
                    "weight",
                ],
            ),
            shares=pick_col(
                frame,
                [
                    "shares",
                    "quantity",
                ],
            ),
            mval=pick_col(
                frame,
                [
                    "market value",
                ],
            ),
            isin=pick_col(
                frame,
                ["isin"],
            ),
            sedol=pick_col(
                frame,
                ["sedol"],
            ),
        )

        result_all, precision = (
            maybe_recompute_from_market_value(
                result_all
            )
        )

        result = drop_nonsecurity_rows(
            result_all
        )

        return FetchResult(
            result,
            download_date
            or source_date,
            precision,
        )

    except Exception as exc:

        errors.append(
            f"direct XLSX: {exc}"
        )

    try:

        page = http_get(
            session,
            VANECK_PAGE_URL,
            timeout,
        )

        source_date = (
            extract_date_from_text(
                page.text[:50000]
            )
            or source_date
        )

        download_url = (
            find_download_link(
                page.text,
                page.url,
                text_contains=(
                    "download",
                    "xls",
                ),
            )
            or
            find_download_link(
                page.text,
                page.url,
                href_contains=(
                    "downloads",
                    "holdings",
                ),
            )
            or
            find_download_link(
                page.text,
                page.url,
                href_contains=(
                    ".xls",
                ),
            )
        )

        if not download_url:

            raise HoldingsError(
                "Download XLS link "
                "not found"
            )

        response = http_get(
            session,
            download_url,
            timeout,
            headers={
                "Referer": (
                    VANECK_PAGE_URL
                )
            },
        )

        frame, download_date = (
            parse_downloaded_holdings(
                response,
                provider="VanEck",
                source_date_hint=(
                    source_date
                ),
            )
        )

        result_all = standardise(
            frame,
            ticker=pick_col(
                frame,
                [
                    "ticker",
                    "symbol",
                ],
            ),
            name=pick_col(
                frame,
                [
                    "holding name",
                    "name",
                    "holding",
                    "security",
                ],
            ),
            weight=pick_col(
                frame,
                [
                    "% of net assets",
                    "% of net",
                    "weight",
                ],
            ),
            shares=pick_col(
                frame,
                [
                    "shares",
                    "quantity",
                ],
            ),
            mval=pick_col(
                frame,
                ["market value"],
            ),
            isin=pick_col(
                frame,
                ["isin"],
            ),
            sedol=pick_col(
                frame,
                ["sedol"],
            ),
        )

        result_all, precision = (
            maybe_recompute_from_market_value(
                result_all
            )
        )

        result = drop_nonsecurity_rows(
            result_all
        )

        return FetchResult(
            result,
            download_date
            or source_date,
            precision,
        )

    except Exception as exc:

        errors.append(
            "page discovery: "
            f"{exc}"
        )

    raise HoldingsError(
        f"{etf}: "
        + " | ".join(errors)
    )


# ============================================================
# INVESCO / SOXQ
# ============================================================

def fetch_invesco(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    errors: list[str] = []

    source_date = iso_today()

    candidates: list[str] = []

    try:

        page = session.get(
            INVESCO_PAGE_URL,
            headers={
                **HEADERS,
                "Accept": (
                    "text/html,"
                    "application/xhtml+xml,"
                    "application/xml;q=0.9,"
                    "*/*;q=0.8"
                ),
            },
            timeout=(
                10,
                timeout,
            ),
            allow_redirects=True,
        )

        if (
            page.status_code == 200
            and
            len(page.content) >= 100
        ):

            source_date = (
                extract_date_from_text(
                    page.text[:50000]
                )
                or source_date
            )

            discovered = (
                find_download_link(
                    page.text,
                    page.url,
                    text_contains=(
                        "export",
                    ),
                )
            )

            if discovered:
                candidates.append(
                    discovered
                )

        else:

            errors.append(
                "product page "
                f"HTTP {page.status_code}"
            )

    except Exception as exc:

        errors.append(
            f"product page: {exc}"
        )

    candidates.append(
        INVESCO_EXPORT_URL.format(
            ticker=etf
        )
    )

    for url in dict.fromkeys(
        candidates
    ):

        try:

            response = session.get(
                url,
                headers={
                    **HEADERS,
                    "Referer": (
                        INVESCO_PAGE_URL
                    ),
                    "Accept": (
                        "text/csv,"
                        "application/csv,"
                        "application/vnd.ms-excel,"
                        "application/vnd.openxmlformats-"
                        "officedocument.spreadsheetml.sheet,"
                        "application/octet-stream;q=0.9,"
                        "*/*;q=0.8"
                    ),
                },
                timeout=(
                    10,
                    timeout,
                ),
                allow_redirects=True,
            )

            if (
                response.status_code
                != 200
            ):

                raise HoldingsError(
                    "HTTP "
                    f"{response.status_code}"
                )

            if len(
                response.content
            ) < 100:

                raise HoldingsError(
                    "response too small"
                )

            frame, download_date = (
                parse_downloaded_holdings(
                    response,
                    provider="Invesco",
                    source_date_hint=(
                        source_date
                    ),
                )
            )

            result_all = standardise(
                frame,
                ticker=pick_col(
                    frame,
                    [
                        "ticker",
                        "symbol",
                    ],
                ),
                name=pick_col(
                    frame,
                    [
                        "holding name",
                        "name",
                        "security",
                        "description",
                    ],
                ),
                weight=pick_col(
                    frame,
                    [
                        "weight",
                        "% of fund",
                        "% of net",
                    ],
                ),
                shares=pick_col(
                    frame,
                    [
                        "shares",
                        "quantity",
                    ],
                ),
                mval=pick_col(
                    frame,
                    ["market value"],
                ),
                isin=pick_col(
                    frame,
                    ["isin"],
                ),
                sedol=pick_col(
                    frame,
                    ["sedol"],
                ),
            )

            result_all, precision = (
                maybe_recompute_from_market_value(
                    result_all
                )
            )

            result = (
                drop_nonsecurity_rows(
                    result_all
                )
            )

            return FetchResult(
                result,
                download_date
                or source_date,
                precision,
            )

        except Exception as exc:

            errors.append(
                f"{url}: {exc}"
            )

    raise HoldingsError(
        f"{etf}: official Invesco "
        "routes unavailable: "
        + " | ".join(
            errors[-6:]
        )
    )


# ============================================================
# VANGUARD JSON
# ============================================================

def walk_dict_lists(
    value: Any,
):

    if isinstance(
        value,
        list,
    ):

        if (
            value
            and
            all(
                isinstance(
                    item,
                    dict,
                )
                for item
                in value
            )
        ):

            yield value

        for item in value:

            yield from walk_dict_lists(
                item
            )

    elif isinstance(
        value,
        dict,
    ):

        for item in (
            value.values()
        ):

            yield from walk_dict_lists(
                item
            )


def score_vanguard_rows(
    rows: list[
        dict[str, Any]
    ],
) -> int:

    score = 0

    for row in rows[:20]:

        keys = {
            str(key).lower()
            for key
            in row
        }

        if (
            "ticker" in keys
            or
            "tickersymbol" in keys
            or
            "symbol" in keys
        ):
            score += 3

        if (
            "sharesheld" in keys
            or
            "shares" in keys
            or
            "quantity" in keys
        ):
            score += 3

        if (
            "marketvalue" in keys
            or
            "marketval" in keys
        ):
            score += 2

        if (
            "longname" in keys
            or
            "shortname" in keys
            or
            "name" in keys
        ):
            score += 1

    return score


def recursive_find_date(
    payload: Any,
) -> str | None:

    date_keys = {
        "asofdate",
        "as_of_date",
        "asof",
        "effectivedate",
        "effective_date",
        "portfoliodate",
        "portfolio_date",
        "date",
    }

    stack = [
        payload
    ]

    while stack:

        item = stack.pop()

        if isinstance(
            item,
            dict,
        ):

            for key, value in (
                item.items()
            ):

                if (
                    str(key)
                    .lower()
                    in date_keys
                ):

                    parsed = (
                        normalize_date_value(
                            value
                        )
                    )

                    if parsed:
                        return parsed

            stack.extend(
                item.values()
            )

        elif isinstance(
            item,
            list,
        ):

            stack.extend(
                item
            )

    return None


def fetch_vanguard(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    url = (
        VANGUARD_URL
        .format(
            ticker=etf.lower()
        )
    )

    response = http_get(
        session,
        url,
        timeout,
    )

    try:

        payload = response.json()

    except json.JSONDecodeError as exc:

        raise HoldingsError(
            f"{etf}: Vanguard "
            "response was not JSON"
        ) from exc

    candidate_lists = list(
        walk_dict_lists(
            payload
        )
    )

    if not candidate_lists:

        raise HoldingsError(
            f"{etf}: no Vanguard "
            "holdings list found"
        )

    entities = max(
        candidate_lists,
        key=score_vanguard_rows,
    )

    if score_vanguard_rows(
        entities
    ) <= 0:

        raise HoldingsError(
            f"{etf}: Vanguard holdings "
            "schema not recognised"
        )

    records: list[
        dict[str, Any]
    ] = []

    for entity in entities:

        ticker = clean_text(
            entity.get("ticker")
            or
            entity.get(
                "tickerSymbol"
            )
            or
            entity.get("symbol")
            or
            ""
        )

        name = clean_text(
            entity.get(
                "longName"
            )
            or
            entity.get(
                "shortName"
            )
            or
            entity.get("name")
            or
            entity.get(
                "securityName"
            )
            or
            ""
        )

        weight = to_float(
            entity.get(
                "percentWeight"
            )
            or
            entity.get("weight")
            or
            entity.get("percent")
        )

        shares = to_float(
            entity.get(
                "sharesHeld"
            )
            or
            entity.get("shares")
            or
            entity.get(
                "quantity"
            )
        )

        market_value = to_float(
            entity.get(
                "marketValue"
            )
            or
            entity.get(
                "marketVal"
            )
            or
            entity.get("value")
        )

        isin = clean_text(
            entity.get("isin")
            or
            ""
        )

        sedol = clean_text(
            entity.get("sedol")
            or
            ""
        )

        records.append(
            {
                "raw_ticker": ticker,
                "raw_name": name,
                "weight": weight,
                "shares": shares,
                "market_value": (
                    market_value
                ),
                "isin": isin,
                "sedol": sedol,
            }
        )

    result = pd.DataFrame(
        records,
        columns=STD_COLS,
    )

    result = drop_nonsecurity_rows(
        result
    )

    source_date = (
        recursive_find_date(
            payload
        )
        or iso_today()
    )

    return FetchResult(
        result,
        source_date,
        "vanguard_snapshot",
    )


# ============================================================
# VGT DRIFT
# ============================================================

def drift_vgt_with_yahoo(
    frame: pd.DataFrame,
    snapshot_date: str,
) -> pd.DataFrame:

    import yfinance as yf

    output = frame.copy()

    if output.empty:

        raise HoldingsError(
            "VGT: Vanguard returned "
            "no holdings"
        )

    try:

        snapshot_day = (
            datetime
            .fromisoformat(
                snapshot_date
            )
            .date()
        )

    except ValueError:

        snapshot_day = (
            utc_now()
            .date()
        )

    start_date = (
        snapshot_day
        - timedelta(days=3)
    ).isoformat()

    output["_yahoo"] = (
        output[
            "raw_ticker"
        ]
        .map(
            yahoo_ticker
        )
    )

    symbols = sorted(
        {
            symbol
            for symbol
            in output["_yahoo"]
            if symbol
        }
    )

    if not symbols:

        raise HoldingsError(
            "VGT: no Yahoo-priceable "
            "tickers"
        )

    print(
        "[VGT] Pricing "
        f"{len(symbols)} securities "
        "via Yahoo...",
        file=sys.stderr,
    )

    try:

        history = yf.download(
            tickers=symbols,
            start=start_date,
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

        raise HoldingsError(
            "VGT: Yahoo bulk "
            f"download failed: {exc}"
        ) from exc

    if (
        history is None
        or
        len(history) == 0
    ):

        raise HoldingsError(
            "VGT: Yahoo returned "
            "no price history"
        )

    closes = _field_frame(
        history,
        "Close",
        symbols,
    )

    splits = _field_frame(
        history,
        "Stock Splits",
        symbols,
    )

    latest_price: dict[
        str,
        float,
    ] = {}

    split_factor: dict[
        str,
        float,
    ] = {}

    for symbol in symbols:

        if symbol in closes.columns:

            series = pd.to_numeric(
                closes[symbol],
                errors="coerce",
            ).dropna()

            if not series.empty:

                latest_price[
                    symbol
                ] = float(
                    series.iloc[-1]
                )

        factor = 1.0

        if symbol in splits.columns:

            series = pd.to_numeric(
                splits[symbol],
                errors="coerce",
            ).fillna(0.0)

            for index, value in (
                series.items()
            ):

                event_day = (
                    pd.Timestamp(index)
                    .date()
                )

                if (
                    event_day
                    > snapshot_day
                    and
                    value > 0
                    and
                    value not in (
                        0,
                        1,
                    )
                ):

                    factor *= float(
                        value
                    )

        split_factor[
            symbol
        ] = factor

    snapshot_market_value = (
        pd.to_numeric(
            output[
                "market_value"
            ],
            errors="coerce",
        )
    )

    snapshot_weight = (
        pd.to_numeric(
            output[
                "weight"
            ],
            errors="coerce",
        )
    )

    if (
        snapshot_weight
        .notna()
        .sum()
        < len(output)
        and
        snapshot_market_value
        .notna()
        .all()
    ):

        total_snapshot = float(
            snapshot_market_value
            .sum()
        )

        if total_snapshot > 0:

            snapshot_weight = (
                snapshot_market_value
                / total_snapshot
                * 100.0
            )

    live_values: list[
        float
    ] = []

    adjusted_shares: list[
        float | None
    ] = []

    fallback_weight = 0.0

    fallback_names: list[
        str
    ] = []

    for index, row in (
        output.iterrows()
    ):

        symbol = row[
            "_yahoo"
        ]

        shares = to_float(
            row["shares"]
        )

        price = latest_price.get(
            symbol
        )

        factor = split_factor.get(
            symbol,
            1.0,
        )

        if (
            shares is not None
            and
            shares > 0
            and
            price is not None
            and
            price > 0
        ):

            current_shares = (
                shares
                * factor
            )

            current_value = (
                current_shares
                * price
            )

            adjusted_shares.append(
                current_shares
            )

            live_values.append(
                current_value
            )

            continue

        snapshot_value = to_float(
            row[
                "market_value"
            ]
        )

        row_weight = 0.0

        try:

            value = (
                snapshot_weight
                .loc[index]
            )

            if pd.notna(value):

                row_weight = float(
                    value
                )

        except Exception:
            pass

        fallback_weight += max(
            row_weight,
            0.0,
        )

        fallback_names.append(
            f"{row['raw_ticker']}"
            f"({row_weight:.4f}%)"
        )

        if (
            snapshot_value is None
            or
            snapshot_value < 0
        ):

            raise HoldingsError(
                "VGT: no live price and "
                "no valid snapshot MV for "
                f"{row['raw_ticker']} "
                f"{row['raw_name']}"
            )

        adjusted_shares.append(
            shares
        )

        live_values.append(
            snapshot_value
        )

    if (
        fallback_weight
        >
        VGT_MAX_TOTAL_FALLBACK_WEIGHT_PCT
    ):

        raise HoldingsError(
            "VGT: unpriced holdings too "
            "material to drift safely: "
            f"{fallback_weight:.4f}% > "
            f"{VGT_MAX_TOTAL_FALLBACK_WEIGHT_PCT:.4f}% "
            f"({', '.join(fallback_names[:10])})"
        )

    total_value = float(
        sum(live_values)
    )

    if total_value <= 0:

        raise HoldingsError(
            "VGT: zero total market value "
            "after repricing"
        )

    output["shares"] = (
        adjusted_shares
    )

    output["market_value"] = (
        live_values
    )

    output["weight"] = (
        pd.Series(
            live_values,
            index=output.index,
        )
        / total_value
        * 100.0
    )

    output = output.drop(
        columns=[
            "_yahoo",
        ]
    )

    if fallback_names:

        print(
            "[VGT] Tiny fallback tail "
            "retained at snapshot value: "
            f"{fallback_weight:.4f}%",
            file=sys.stderr,
        )

    return output


# ============================================================
# IDENTITY / TICKER COLLISION PROTECTION
# ============================================================

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


def canonical_ticker(
    raw: str,
) -> str:

    ticker = (
        clean_text(raw)
        .upper()
    )

    if not ticker:
        return ""

    ticker = re.sub(
        r"\s+(US|UN|UW|UQ|UR|UF)$",
        "",
        ticker,
    )

    ticker = ticker.replace(
        "/",
        "-",
    )

    ticker = re.sub(
        r"\s+",
        "-",
        ticker,
    )

    ticker = re.sub(
        r"-+",
        "-",
        ticker,
    )

    return ticker.strip("-")


def normalized_name(
    raw: str,
) -> str:

    text = unicodedata.normalize(
        "NFKD",
        clean_text(raw),
    )

    text = (
        text
        .encode(
            "ascii",
            "ignore",
        )
        .decode()
    )

    text = text.upper()

    text = re.sub(
        r"[^A-Z0-9& ]+",
        " ",
        text,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    changed = True

    while changed and text:

        changed = False

        for pattern in (
            LEGAL_SUFFIX_PATTERNS
        ):

            updated = re.sub(
                rf"(?:\s+{pattern})$",
                "",
                text,
            ).strip()

            if updated != text:

                text = updated
                changed = True

    return text


def valid_isin(
    value: str,
) -> bool:

    return bool(
        re.fullmatch(
            r"[A-Z]{2}[A-Z0-9]{9}[0-9]",
            clean_text(value).upper(),
        )
    )


def valid_sedol(
    value: str,
) -> bool:

    return bool(
        re.fullmatch(
            r"[A-Z0-9]{7}",
            clean_text(value).upper(),
        )
    )


def assign_identity_keys(
    long_df: pd.DataFrame,
) -> pd.DataFrame:

    frame = long_df.copy()

    frame["_ticker"] = (
        frame[
            "raw_ticker"
        ]
        .map(
            canonical_ticker
        )
    )

    frame["_name_norm"] = (
        frame[
            "raw_name"
        ]
        .map(
            normalized_name
        )
    )

    frame["_isin"] = (
        frame["isin"]
        .map(
            lambda value:
            clean_text(value)
            .upper()
        )
    )

    frame["_sedol"] = (
        frame["sedol"]
        .map(
            lambda value:
            clean_text(value)
            .upper()
        )
    )

    ticker_to_strong: dict[
        str,
        set[str],
    ] = {}

    for _, row in (
        frame.iterrows()
    ):

        ticker = row[
            "_ticker"
        ]

        if not ticker:
            continue

        strong_id = None

        if valid_isin(
            row["_isin"]
        ):

            strong_id = (
                "ISIN:"
                + row["_isin"]
            )

        elif valid_sedol(
            row["_sedol"]
        ):

            strong_id = (
                "SEDOL:"
                + row["_sedol"]
            )

        if strong_id:

            ticker_to_strong.setdefault(
                ticker,
                set(),
            ).add(
                strong_id
            )

    unique_ticker_alias = {
        ticker:
        next(
            iter(strong_ids)
        )
        for ticker, strong_ids
        in ticker_to_strong.items()
        if len(strong_ids) == 1
    }

    def identity(
        row: pd.Series,
    ) -> str:

        if valid_isin(
            row["_isin"]
        ):

            return (
                "ISIN:"
                + row["_isin"]
            )

        if valid_sedol(
            row["_sedol"]
        ):

            return (
                "SEDOL:"
                + row["_sedol"]
            )

        ticker = row[
            "_ticker"
        ]

        if (
            ticker
            and
            ticker
            in unique_ticker_alias
        ):

            return (
                unique_ticker_alias[
                    ticker
                ]
            )

        if ticker:

            return (
                f"TK:{ticker}|"
                f"{row['_name_norm']}"
            )

        return (
            "NM:"
            + row["_name_norm"]
        )

    frame["_identity"] = (
        frame.apply(
            identity,
            axis=1,
        )
    )

    return frame


def choose_canonical_name(
    group: pd.DataFrame,
) -> str:

    names = [
        clean_text(name)
        for name
        in group["raw_name"]
        if clean_text(name)
    ]

    if not names:

        tickers = [
            ticker
            for ticker
            in group["_ticker"]
            if ticker
        ]

        if tickers:
            return tickers[0]

        return ""

    counts = (
        pd.Series(names)
        .value_counts()
    )

    highest = int(
        counts.iloc[0]
    )

    tied = [
        name
        for name, count
        in counts.items()
        if int(count)
        == highest
    ]

    def score(
        name: str,
    ) -> tuple:

        mixed_case = not (
            name.isupper()
            or
            name.islower()
        )

        return (
            0
            if mixed_case
            else 1,
            len(name),
            name.upper(),
        )

    return sorted(
        tied,
        key=score,
    )[0]


def choose_canonical_ticker(
    group: pd.DataFrame,
) -> str:

    tickers = [
        ticker
        for ticker
        in group["_ticker"]
        if ticker
    ]

    if not tickers:
        return ""

    counts = (
        pd.Series(tickers)
        .value_counts()
    )

    highest = int(
        counts.iloc[0]
    )

    tied = [
        ticker
        for ticker, count
        in counts.items()
        if int(count)
        == highest
    ]

    qualified = [
        ticker
        for ticker
        in tied
        if "." in ticker
    ]

    if qualified:

        return sorted(
            qualified
        )[0]

    return sorted(
        tied,
        key=lambda ticker: (
            len(ticker),
            ticker,
        ),
    )[0]


def canonicalize_across_etfs(
    long_df: pd.DataFrame,
) -> pd.DataFrame:

    frame = assign_identity_keys(
        long_df
    )

    mapping: dict[
        str,
        tuple[
            str,
            str,
        ],
    ] = {}

    for identity, group in (
        frame.groupby(
            "_identity",
            sort=False,
        )
    ):

        mapping[
            str(identity)
        ] = (
            choose_canonical_ticker(
                group
            ),
            choose_canonical_name(
                group
            ),
        )

    frame[
        "canonical_ticker"
    ] = frame[
        "_identity"
    ].map(
        lambda key:
        mapping[
            str(key)
        ][0]
    )

    frame[
        "canonical_name"
    ] = frame[
        "_identity"
    ].map(
        lambda key:
        mapping[
            str(key)
        ][1]
    )

    frame[
        "canonical_ticker"
    ] = frame[
        "canonical_ticker"
    ].where(
        frame[
            "canonical_ticker"
        ].ne(""),
        frame[
            "raw_ticker"
        ].map(
            canonical_ticker
        ),
    )

    frame[
        "canonical_name"
    ] = frame[
        "canonical_name"
    ].where(
        frame[
            "canonical_name"
        ].ne(""),
        frame[
            "raw_name"
        ].map(
            clean_text
        ),
    )

    return frame


# ============================================================
# LIVE VALIDATION
# ============================================================

def validate_live_frame(
    etf: str,
    frame: pd.DataFrame,
    previous_count: int | None,
) -> None:

    if frame.empty:

        raise HoldingsError(
            f"{etf}: no holdings"
        )

    floor = (
        MIN_HOLDINGS_BY_TICKER[
            etf
        ]
    )

    required = floor

    if (
        previous_count
        and
        previous_count >= floor
    ):

        required = max(
            floor,
            math.floor(
                previous_count
                * MIN_PREVIOUS_COUNT_RATIO
            ),
        )

    if len(frame) < required:

        raise HoldingsError(
            f"{etf}: only {len(frame)} rows; "
            f"required at least {required}"
        )

    weights = pd.to_numeric(
        frame["weight"],
        errors="coerce",
    )

    numeric_count = int(
        weights
        .notna()
        .sum()
    )

    minimum_numeric = (
        math.floor(
            len(frame)
            * 0.98
        )
    )

    if (
        numeric_count
        < minimum_numeric
    ):

        raise HoldingsError(
            f"{etf}: only "
            f"{numeric_count}/{len(frame)} "
            "rows have numeric weights"
        )

    if (
        weights
        .dropna()
        .lt(-0.01)
        .any()
    ):

        raise HoldingsError(
            f"{etf}: materially negative "
            "holding weight"
        )

    total = float(
        weights
        .sum(
            skipna=True
        )
    )

    if not (
        MIN_TOTAL_WEIGHT
        <= total
        <= MAX_TOTAL_WEIGHT
    ):

        raise HoldingsError(
            f"{etf}: weight sum "
            f"{total:.8f}% outside "
            f"[{MIN_TOTAL_WEIGHT}, "
            f"{MAX_TOTAL_WEIGHT}]"
        )

    names = (
        frame[
            "raw_name"
        ]
        .fillna("")
        .astype(str)
    )

    if names.str.contains(
        r"<[^>]+>",
        regex=True,
    ).any():

        raise HoldingsError(
            f"{etf}: HTML leaked "
            "into holding names"
        )

    duplicated = (
        frame
        .duplicated(
            subset=[
                "raw_ticker",
                "raw_name",
                "shares",
                "market_value",
            ],
            keep=False,
        )
    )

    if (
        int(
            duplicated.sum()
        )
        >
        max(
            4,
            int(
                len(frame)
                * 0.02
            ),
        )
    ):

        raise HoldingsError(
            f"{etf}: suspicious "
            "duplicate-row count "
            f"{int(duplicated.sum())}"
        )


# ============================================================
# PREVIOUS FILE
# ============================================================

def load_previous(
    combined_path: Path,
) -> dict[
    str,
    pd.DataFrame,
]:

    if not combined_path.exists():
        return {}

    try:

        previous = pd.read_csv(
            combined_path
        )

    except Exception as exc:

        print(
            "WARNING: could not read "
            "last-known-good CSV: "
            f"{exc}",
            file=sys.stderr,
        )

        return {}

    if any(
        column
        not in previous.columns
        for column
        in OUTPUT_COLS
    ):

        print(
            "WARNING: previous CSV schema "
            "does not match expected schema",
            file=sys.stderr,
        )

        return {}

    result: dict[
        str,
        pd.DataFrame,
    ] = {}

    for ticker, group in (
        previous.groupby(
            "fund_ticker",
            sort=False,
        )
    ):

        result[
            str(ticker).upper()
        ] = (
            group[
                OUTPUT_COLS
            ]
            .copy()
            .reset_index(
                drop=True
            )
        )

    return result


# ============================================================
# FALLBACK CLEANUP
# ============================================================

def sanitize_previous_output(
    frame: pd.DataFrame,
) -> pd.DataFrame:

    output = (
        frame[
            OUTPUT_COLS
        ]
        .copy()
    )

    output[
        "holding_ticker"
    ] = (
        output[
            "holding_ticker"
        ]
        .map(
            clean_text
        )
    )

    output[
        "holding_name"
    ] = (
        output[
            "holding_name"
        ]
        .map(
            clean_text
        )
    )

    keep = [
        not is_nonsecurity(
            ticker,
            name,
        )
        for ticker, name
        in zip(
            output[
                "holding_ticker"
            ],
            output[
                "holding_name"
            ],
        )
    ]

    output = (
        output.loc[keep]
        .reset_index(
            drop=True
        )
    )

    output[
        "rank"
    ] = range(
        1,
        len(output) + 1,
    )

    return output


def fill_output_market_values_from_yahoo(
    frame: pd.DataFrame,
    *,
    label: str,
    retrieved_at: datetime,
) -> pd.DataFrame:

    output = frame.copy()

    market_value = pd.to_numeric(
        output[
            "market_value_usd"
        ],
        errors="coerce",
    )

    shares = pd.to_numeric(
        output[
            "shares_held"
        ],
        errors="coerce",
    )

    need = (
        market_value.isna()
        &
        shares.notna()
        &
        (shares > 0)
        &
        output[
            "holding_ticker"
        ]
        .astype(str)
        .ne("")
    )

    if not need.any():
        return output

    required_tickers = (
        output.loc[
            need,
            "holding_ticker",
        ]
        .map(
            clean_text
        )
    )

    prices = yahoo_latest_prices(
        required_tickers
    )

    filled = 0

    for index in (
        output.index[need]
    ):

        ticker = clean_text(
            output.at[
                index,
                "holding_ticker",
            ]
        )

        shares_held = to_float(
            output.at[
                index,
                "shares_held",
            ]
        )

        price = prices.get(
            ticker
        )

        if (
            price is None
            or
            shares_held is None
            or
            shares_held <= 0
        ):
            continue

        output.at[
            index,
            "market_value_usd",
        ] = (
            shares_held
            * price
        )

        filled += 1

    if filled:

        output[
            "retrieved_at_utc"
        ] = (
            retrieved_at
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )

    remaining = int(
        pd.to_numeric(
            output[
                "market_value_usd"
            ],
            errors="coerce",
        )
        .isna()
        .sum()
    )

    print(
        f"[{label}] Fallback Yahoo "
        f"MV backfill: {filled} filled, "
        f"{remaining} blank",
        file=sys.stderr,
    )

    return output


# ============================================================
# OUTPUT HTML GUARD
# ============================================================

def assert_no_html_remnants(
    frame: pd.DataFrame,
) -> None:

    names = (
        frame[
            "holding_name"
        ]
        .fillna("")
        .astype(str)
    )

    bad = names.str.contains(
        (
            r"<[^>]+>"
            r"|"
            r"&(?:"
            r"[A-Za-z]+"
            r"|#\d+"
            r"|#x[0-9A-Fa-f]+"
            r");"
        ),
        regex=True,
    )

    if not bad.any():
        return

    sample = (
        frame.loc[
            bad,
            [
                "fund_ticker",
                "holding_ticker",
                "holding_name",
            ],
        ]
        .head(10)
        .to_dict(
            orient="records"
        )
    )

    raise HoldingsError(
        "HTML/entity remnants survived "
        "holding-name cleanup: "
        f"{sample}"
    )


# ============================================================
# OUTPUT VALIDATION
# ============================================================

def validate_output_for_etf(
    etf: str,
    frame: pd.DataFrame,
) -> None:

    if frame.empty:

        raise HoldingsError(
            f"{etf}: output is empty"
        )

    floor = (
        MIN_HOLDINGS_BY_TICKER[
            etf
        ]
    )

    if len(frame) < floor:

        raise HoldingsError(
            f"{etf}: only "
            f"{len(frame)} rows "
            f"(< {floor})"
        )

    weights = pd.to_numeric(
        frame[
            "weight"
        ],
        errors="coerce",
    )

    total = float(
        weights.sum(
            skipna=True
        )
    )

    if not (
        MIN_TOTAL_WEIGHT
        <= total
        <= MAX_TOTAL_WEIGHT
    ):

        raise HoldingsError(
            f"{etf}: weight sum "
            f"{total:.8f}% invalid"
        )


# ============================================================
# NORMALISED OUTPUT
# ============================================================

def normalized_output_for_etf(
    etf: str,
    canonical_df: pd.DataFrame,
    source_date: str,
    retrieved_at: datetime,
) -> pd.DataFrame:

    fund = (
        canonical_df[
            canonical_df[
                "etf"
            ].eq(etf)
        ]
        .copy()
    )

    fund["weight"] = pd.to_numeric(
        fund["weight"],
        errors="coerce",
    )

    fund["shares"] = pd.to_numeric(
        fund["shares"],
        errors="coerce",
    )

    fund[
        "market_value"
    ] = pd.to_numeric(
        fund[
            "market_value"
        ],
        errors="coerce",
    )

    fund = (
        fund
        .sort_values(
            "weight",
            ascending=False,
            na_position="last",
        )
        .reset_index(
            drop=True
        )
    )

    output = pd.DataFrame(
        {
            "source_date": (
                source_date
            ),
            "retrieved_at_utc": (
                retrieved_at
                .isoformat()
                .replace(
                    "+00:00",
                    "Z",
                )
            ),
            "fund_ticker": etf,
            "provider": (
                PROVIDER_LABELS[
                    etf
                ]
            ),
            "rank": range(
                1,
                len(fund) + 1,
            ),
            "holding_ticker": (
                fund[
                    "canonical_ticker"
                ]
            ),
            "holding_name": (
                fund[
                    "canonical_name"
                ]
            ),
            "weight": (
                fund[
                    "weight"
                ]
            ),
            "shares_held": (
                fund[
                    "shares"
                ]
            ),
            "market_value_usd": (
                fund[
                    "market_value"
                ]
            ),
        }
    )

    return output[
        OUTPUT_COLS
    ]


# ============================================================
# ATOMIC WRITE
# ============================================================

def write_csv_atomic(
    frame: pd.DataFrame,
    destination: Path,
) -> None:

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = (
        destination
        .with_name(
            "."
            + destination.name
            + ".tmp"
        )
    )

    try:

        frame.to_csv(
            temp_path,
            index=False,
            encoding="utf-8-sig",
            float_format="%.15g",
        )

        if (
            not temp_path.exists()
            or
            temp_path.stat().st_size
            == 0
        ):

            raise HoldingsError(
                "Generated CSV is empty"
            )

        os.replace(
            temp_path,
            destination,
        )

    finally:

        if temp_path.exists():

            try:
                temp_path.unlink()
            except Exception:
                pass


# ============================================================
# FETCHER REGISTRY
# ============================================================

Fetcher = Callable[
    [
        requests.Session,
        str,
        int,
    ],
    FetchResult,
]

FETCHERS: dict[
    str,
    Fetcher,
] = {
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


# ============================================================
# FETCH LIVE
# ============================================================

def fetch_all_live(
    session: requests.Session,
    etfs: list[str],
    previous: dict[
        str,
        pd.DataFrame,
    ],
    timeout: int,
) -> tuple[
    dict[str, pd.DataFrame],
    dict[str, str],
    dict[str, str],
    dict[str, str],
]:

    live: dict[
        str,
        pd.DataFrame,
    ] = {}

    source_dates: dict[
        str,
        str,
    ] = {}

    precision_methods: dict[
        str,
        str,
    ] = {}

    failures: dict[
        str,
        str,
    ] = {}

    for etf in etfs:

        provider = (
            PROVIDER_LABELS[
                etf
            ]
        )

        print(
            f"[{etf}] Fetching "
            f"{provider}...",
            file=sys.stderr,
        )

        try:

            result = (
                FETCHERS[
                    etf
                ](
                    session,
                    etf,
                    timeout,
                )
            )

            frame = (
                result.frame
                .copy()
            )

            if etf == "VGT":

                frame = (
                    drift_vgt_with_yahoo(
                        frame,
                        result.source_date,
                    )
                )

                result.precision_method = (
                    "vanguard_shares_x_live_price"
                )

            previous_count = (
                len(
                    previous[
                        etf
                    ]
                )
                if etf
                in previous
                else None
            )

            validate_live_frame(
                etf,
                frame,
                previous_count,
            )

            frame["etf"] = etf

            live[
                etf
            ] = frame

            source_dates[
                etf
            ] = (
                result.source_date
            )

            precision_methods[
                etf
            ] = (
                result.precision_method
            )

            total_weight = float(
                pd.to_numeric(
                    frame[
                        "weight"
                    ],
                    errors="coerce",
                )
                .sum(
                    skipna=True
                )
            )

            print(
                f"[{etf}] OK: "
                f"{len(frame)} rows, "
                f"{total_weight:.8f}% "
                f"({result.precision_method})",
                file=sys.stderr,
            )

        except Exception as exc:

            failures[
                etf
            ] = str(exc)

            print(
                f"[{etf}] LIVE FAILED: "
                f"{exc}",
                file=sys.stderr,
            )

    return (
        live,
        source_dates,
        precision_methods,
        failures,
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

def run(
    etfs: list[str],
    out_dir: Path,
    combined_name: str,
    timeout: int,
) -> int:

    combined_path = (
        out_dir
        / combined_name
    )

    previous = load_previous(
        combined_path
    )

    with build_session() as session:

        (
            live,
            source_dates,
            precision_methods,
            live_failures,
        ) = fetch_all_live(
            session,
            etfs,
            previous,
            timeout,
        )

    canonical_live: (
        pd.DataFrame
        | None
    ) = None

    if live:

        stacked = pd.concat(
            [
                live[etf]
                for etf
                in etfs
                if etf in live
            ],
            ignore_index=True,
        )

        canonical_live = (
            canonicalize_across_etfs(
                stacked
            )
        )

    retrieved_at = utc_now()

    final_by_etf: dict[
        str,
        pd.DataFrame,
    ] = {}

    source_status: dict[
        str,
        str,
    ] = {}

    fatal: dict[
        str,
        str,
    ] = {}

    for etf in etfs:

        if (
            etf in live
            and
            canonical_live
            is not None
        ):

            output = (
                normalized_output_for_etf(
                    etf,
                    canonical_live,
                    source_dates[
                        etf
                    ],
                    retrieved_at,
                )
            )

            validate_output_for_etf(
                etf,
                output,
            )

            final_by_etf[
                etf
            ] = output

            source_status[
                etf
            ] = (
                PROVIDER_LABELS[
                    etf
                ]
                + " / "
                + precision_methods[
                    etf
                ]
            )

            continue

        if etf in previous:

            try:

                fallback = (
                    sanitize_previous_output(
                        previous[
                            etf
                        ].copy()
                    )
                )

                validate_output_for_etf(
                    etf,
                    fallback,
                )

                fallback = (
                    fill_output_market_values_from_yahoo(
                        fallback,
                        label=etf,
                        retrieved_at=(
                            retrieved_at
                        ),
                    )
                )

                assert_no_html_remnants(
                    fallback
                )

                final_by_etf[
                    etf
                ] = fallback[
                    OUTPUT_COLS
                ]

                source_status[
                    etf
                ] = (
                    "Last-known-good / "
                    "cleaned + MV backfill"
                )

                print(
                    f"[{etf}] Using cleaned "
                    "last-known-good rows; "
                    "original source_date "
                    "preserved",
                    file=sys.stderr,
                )

                continue

            except Exception as exc:

                fatal[
                    etf
                ] = (
                    "live failed "
                    f"({live_failures.get(etf, 'unknown')}); "
                    "previous invalid "
                    f"({exc})"
                )

        else:

            fatal[
                etf
            ] = (
                "live failed "
                f"({live_failures.get(etf, 'unknown')}); "
                "no last-known-good rows"
            )

    if fatal:

        print(
            "\nUpdate aborted. Existing "
            "combined CSV was NOT replaced.",
            file=sys.stderr,
        )

        for etf, reason in (
            fatal.items()
        ):

            print(
                f"  - {etf}: {reason}",
                file=sys.stderr,
            )

        return 1

    combined = pd.concat(
        [
            final_by_etf[
                etf
            ]
            for etf
            in etfs
        ],
        ignore_index=True,
    )

    actual_funds = set(
        combined[
            "fund_ticker"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    expected_funds = set(
        etfs
    )

    if actual_funds != expected_funds:

        print(
            "ERROR: combined output ETF set "
            "does not match request. "
            f"Expected={expected_funds}, "
            f"Actual={actual_funds}",
            file=sys.stderr,
        )

        return 1

    if (
        list(
            combined.columns
        )
        != OUTPUT_COLS
    ):

        print(
            "ERROR: combined schema changed: "
            f"{list(combined.columns)}",
            file=sys.stderr,
        )

        return 1

    try:

        assert_no_html_remnants(
            combined
        )

    except Exception as exc:

        print(
            f"ERROR: {exc}",
            file=sys.stderr,
        )

        return 1

    market_value = pd.to_numeric(
        combined[
            "market_value_usd"
        ],
        errors="coerce",
    )

    blanks = (
        market_value.isna()
    )

    if blanks.any():

        print(
            "\nMarket-value blanks "
            "remaining by fund:",
            file=sys.stderr,
        )

        counts = (
            combined.loc[
                blanks
            ]
            .groupby(
                "fund_ticker"
            )
            .size()
            .sort_values(
                ascending=False
            )
        )

        for fund, count in (
            counts.items()
        ):

            print(
                f"  - {fund}: "
                f"{count}",
                file=sys.stderr,
            )

    else:

        print(
            "\nMarket-value coverage: "
            "100.00%",
            file=sys.stderr,
        )

    for etf, group in (
        combined.groupby(
            "fund_ticker",
            sort=False,
        )
    ):

        try:

            validate_output_for_etf(
                str(etf),
                group,
            )

        except Exception as exc:

            print(
                f"ERROR: final validation "
                f"failed for {etf}: "
                f"{exc}",
                file=sys.stderr,
            )

            return 1

    # Extra final ACWI precision safeguard.
    #
    # Even if another code path changes later, ACWI must have
    # materially more precision than a 2dp provider export.
    if "ACWI" in expected_funds:

        acwi = combined[
            combined[
                "fund_ticker"
            ].eq("ACWI")
        ].copy()

        acwi_weights = pd.to_numeric(
            acwi["weight"],
            errors="coerce",
        ).dropna()

        if not acwi_weights.empty:

            # A value differs from its 2dp rounding if genuine
            # reconstructed precision is present.
            higher_precision = (
                (
                    acwi_weights
                    -
                    acwi_weights.round(2)
                )
                .abs()
                > 1e-10
            )

            high_precision_count = int(
                higher_precision.sum()
            )

            # Require meaningful evidence that ACWI was not
            # published as the rounded provider-weight file.
            if high_precision_count < 100:

                print(
                    "ERROR: ACWI appears to have reverted "
                    "to rounded provider weights. "
                    f"Only {high_precision_count} holdings "
                    "contain precision beyond 2 decimals.",
                    file=sys.stderr,
                )

                return 1

    write_csv_atomic(
        combined,
        combined_path,
    )

    print(
        "\nPublished "
        f"{len(combined):,} rows "
        f"across {len(etfs)} ETFs "
        f"-> {combined_path}",
        file=sys.stderr,
    )

    print(
        "\nSources used:",
        file=sys.stderr,
    )

    for etf in etfs:

        print(
            f"  - {etf}: "
            f"{source_status[etf]}",
            file=sys.stderr,
        )

    return 0


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Update provider-first "
            "ETF holdings CSV."
        )
    )

    parser.add_argument(
        "tickers",
        nargs="*",
        help=(
            "ETF tickers. "
            "Defaults to the standard "
            "nine-fund basket."
        ),
    )

    parser.add_argument(
        "--out-dir",
        type=Path,
        default=(
            DEFAULT_OUT_DIR
        ),
    )

    parser.add_argument(
        "--combined-name",
        default=(
            DEFAULT_COMBINED_NAME
        ),
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=(
            DEFAULT_TIMEOUT_SECONDS
        ),
    )

    return parser.parse_args()


def main() -> int:

    args = parse_args()

    if (
        Path(
            args.combined_name
        ).name
        != args.combined_name
    ):

        print(
            "ERROR: --combined-name must "
            "be a filename, not a path",
            file=sys.stderr,
        )

        return 2

    if args.timeout <= 0:

        print(
            "ERROR: --timeout must be > 0",
            file=sys.stderr,
        )

        return 2

    if args.tickers:

        etfs = [
            ticker
            .strip()
            .upper()
            for ticker
            in args.tickers
            if ticker.strip()
        ]

    else:

        etfs = (
            DEFAULT_ETFS.copy()
        )

    etfs = list(
        dict.fromkeys(
            etfs
        )
    )

    unknown = [
        etf
        for etf
        in etfs
        if etf
        not in FETCHERS
    ]

    if unknown:

        print(
            "ERROR: unknown ETF(s): "
            f"{unknown}. "
            "Known ETFs: "
            f"{list(FETCHERS)}",
            file=sys.stderr,
        )

        return 2

    return run(
        etfs,
        args.out_dir,
        args.combined_name,
        args.timeout,
    )


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
