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
# TYPES / ERRORS
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

    adapter = HTTPAdapter(
        max_retries=retry
    )

    session = requests.Session()
    session.headers.update(HEADERS)

    session.mount(
        "https://",
        adapter,
    )

    session.mount(
        "http://",
        adapter,
    )

    return session


def http_get(
    session: requests.Session,
    url: str,
    timeout: int,
    *,
    headers: dict[str, str] | None = None,
) -> requests.Response:

    request_headers = (
        HEADERS.copy()
    )

    if headers:
        request_headers.update(
            headers
        )

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
# CLEANING
# ============================================================

def clean_text(
    value: Any,
) -> str:

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

    # Decode HTML entities.
    text = html_lib.unescape(
        text
    )

    # Old fallback rows can contain tooltip HTML such as:
    #
    # <span title="Novo Nordisk A/S">
    #     Novo Nordisk A/..
    # </span>
    #
    # Recover the actual full name first.
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

    text = html_lib.unescape(
        text
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    return text.strip()


def to_float(
    value: Any,
) -> float | None:

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


# ============================================================
# DATE PARSING
# ============================================================

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

    if isinstance(
        value,
        date,
    ):
        return value.isoformat()

    text = clean_text(
        value
    )

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

                year = int(
                    match.group(1)
                )

                month = int(
                    match.group(2)
                )

                day = int(
                    match.group(3)
                )

            else:

                month = int(
                    match.group(1)
                )

                day = int(
                    match.group(2)
                )

                year = int(
                    match.group(3)
                )

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
                .strptime(
                    text,
                    fmt,
                )
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


# ============================================================
# COLUMNS / STANDARDISATION
# ============================================================

def pick_col(
    df: pd.DataFrame,
    candidates: list[str],
) -> str | None:

    columns = [
        (
            str(column),
            clean_text(
                column
            ).lower(),
        )
        for column
        in df.columns
    ]

    # Exact match first.
    for candidate in candidates:

        candidate_lower = (
            candidate.lower()
        )

        for original, lowered in columns:

            if lowered == candidate_lower:
                return original

    # Then substring match.
    for candidate in candidates:

        candidate_lower = (
            candidate.lower()
        )

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

        output[
            "raw_ticker"
        ] = (
            df[ticker]
            .map(clean_text)
        )

    else:
        output[
            "raw_ticker"
        ] = ""

    if name and name in df.columns:

        output[
            "raw_name"
        ] = (
            df[name]
            .map(clean_text)
        )

    else:
        output[
            "raw_name"
        ] = ""

    if weight and weight in df.columns:

        output[
            "weight"
        ] = (
            df[weight]
            .map(to_float)
        )

    else:
        output[
            "weight"
        ] = pd.NA

    if shares and shares in df.columns:

        output[
            "shares"
        ] = (
            df[shares]
            .map(to_float)
        )

    else:
        output[
            "shares"
        ] = pd.NA

    if mval and mval in df.columns:

        output[
            "market_value"
        ] = (
            df[mval]
            .map(to_float)
        )

    else:
        output[
            "market_value"
        ] = pd.NA

    if isin and isin in df.columns:

        output[
            "isin"
        ] = (
            df[isin]
            .map(clean_text)
        )

    else:
        output[
            "isin"
        ] = ""

    if sedol and sedol in df.columns:

        output[
            "sedol"
        ] = (
            df[sedol]
            .map(clean_text)
        )

    else:
        output[
            "sedol"
        ] = ""

    output = output[
        (
            output[
                "raw_ticker"
            ].ne("")
            |
            output[
                "raw_name"
            ].ne("")
        )
    ].copy()

    output[
        "raw_ticker"
    ] = (
        output[
            "raw_ticker"
        ]
        .astype(str)
        .str.strip()
    )

    output[
        "raw_name"
    ] = (
        output[
            "raw_name"
        ]
        .astype(str)
        .str.strip()
    )

    output[
        "isin"
    ] = (
        output[
            "isin"
        ]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    output[
        "sedol"
    ] = (
        output[
            "sedol"
        ]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    return (
        output[
            STD_COLS
        ]
        .reset_index(
            drop=True
        )
    )


# ============================================================
# NON-SECURITY FILTER
# ============================================================

def is_nonsecurity(
    ticker: str,
    name: str,
) -> bool:

    ticker_upper = (
        clean_text(
            ticker
        )
        .upper()
        .strip()
    )

    name_upper = (
        clean_text(
            name
        )
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
        "-",
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

    if ticker_upper.startswith(
        "$"
    ):
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

    if (
        "SECURITIES LENDING"
        in name_upper
    ):
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

    output[
        "raw_ticker"
    ] = (
        output[
            "raw_ticker"
        ]
        .map(clean_text)
    )

    output[
        "raw_name"
    ] = (
        output[
            "raw_name"
        ]
        .map(clean_text)
    )

    keep = [
        not is_nonsecurity(
            ticker,
            name,
        )
        for ticker, name
        in zip(
            output[
                "raw_ticker"
            ],
            output[
                "raw_name"
            ],
        )
    ]

    return (
        output
        .loc[keep]
        .reset_index(
            drop=True
        )
    )


# ============================================================
# WEIGHT RECOMPUTATION
# ============================================================

def maybe_recompute_from_market_value(
    df: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    str,
]:
    """
    Reconstruct high-precision weights from provider market values.

    Important:
    ACWI can contain negative cash balances.

    Negative values are therefore allowed for cash / FX /
    non-security rows, but not for ordinary securities.

    The signed portfolio market value is used as the
    denominator BEFORE cash rows are removed.
    """

    output = df.copy()

    if output.empty:
        return (
            output,
            "provider_weight",
        )

    market_value = pd.to_numeric(
        output[
            "market_value"
        ],
        errors="coerce",
    )

    # Need complete market-value coverage.
    if not market_value.notna().all():

        return (
            output,
            "provider_weight",
        )

    negative_mask = (
        market_value < 0
    )

    if negative_mask.any():

        for idx in (
            output.index[
                negative_mask
            ]
        ):

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

            # Negative cash/FX is OK.
            # Negative actual securities are not.
            if not is_nonsecurity(
                ticker,
                name,
            ):

                return (
                    output,
                    "provider_weight",
                )

    total = float(
        market_value.sum()
    )

    if (
        not math.isfinite(
            total
        )
        or
        total <= 0
    ):

        return (
            output,
            "provider_weight",
        )

    output[
        "weight"
    ] = (
        market_value
        / total
        * 100.0
    )

    return (
        output,
        "market_value_recomputed",
    )


# ============================================================
# CSV / EXCEL PARSERS
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

        lowered = (
            line.lower()
        )

        if all(
            token.lower()
            in lowered
            for token
            in must_contain
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
                lines[
                    header_index:
                ]
            )
        ),
        dtype=str,
    )

    frame.columns = [
        clean_text(
            column
        )
        for column
        in frame.columns
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
        io.BytesIO(
            content
        ),
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
            clean_text(
                value
            ).lower()
            for value
            in raw.iloc[
                index
            ].tolist()
        )

        if all(
            token.lower()
            in row_text
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

    for index in range(
        header_index
    ):

        row_text = " ".join(
            clean_text(
                value
            )
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
        clean_text(
            column
        )
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
        str(
            response.url
        )
        .lower()
    )

    is_excel = (
        "spreadsheet"
        in content_type
        or
        "excel"
        in content_type
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
        in text[
            :10000
        ].lower()
        and
        ","
        in text[
            :3000
        ]
    ):

        frame = sniff_csv(
            text,
            ["ticker"],
        )

        source_date = (
            extract_date_from_text(
                text[
                    :20000
                ]
            )
            or
            source_date_hint
        )

        return (
            frame,
            source_date,
        )

    raise HoldingsError(
    f"{provider}: response was not "
}: response was not "
    "recognisable CSV or Excel "
    f"(content-type={content_type}, "
    f"url={response.url})"
)


# ============================================================
# LINK DISCOVERY
# ============================================================

def find_download_link(
    html_text: str,
    base_url: str,
    *,
    text_contains: tuple[str, ...] = (),
    href_contains: tuple[str, ...] = (),
) -> str | None:

    try:

        root = (
            lxml_html
            .fromstring(
                html_text
            )
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
            anchor.get(
                "href"
            )
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
            clean_text(
                part
            )
            for part
            in anchor.itertext()
        ).lower()

        href_lower = (
            href.lower()
        )

        if text_tokens:

            if not all(
                token
                in label
                for token
                in text_tokens
            ):
                continue

        if href_tokens:

            if not all(
                token
                in href_lower
                for token
                in href_tokens
            ):
                continue

        score = 0

        if (
            "download"
            in label
        ):
            score += 3

        if (
            "full holdings"
            in label
        ):
            score += 3

        if href_lower.endswith(
            (
                ".csv",
               
