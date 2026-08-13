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
import html as html_lib
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
    "https://www.ssga.com/us/en/intermediary/etfs/"
    "library-content/products/fund-data/etfs/us/"
    "holdings-daily-us-en-{t}.xlsx"
)

ISHARES_ACWI_URLS = [
    "https://www.ishares.com/us/products/239600/"
    "ishares-msci-acwi-etf/latest-holdings.csv"
]

INVESCO_PAGE_URL = (
    "https://www.invesco.com/us/en/financial-products/"
    "etfs/invesco-phlx-semiconductor-etf.html"
)

INVESCO_URLS = [
    (
        "https://www.invesco.com/us/financial-products/"
        "etfs/holdings/main/holdings/0"
        "?audienceType=Investor"
        "&action=download"
        "&ticker={t}"
    )
]

GLOBALX_PAGE_URL = "https://www.globalxetfs.com/funds/{t}"

FIRSTTRUST_URLS = [
    "https://www.ftportfolios.com/Retail/Etf/"
    "EtfHoldings.aspx?Ticker={t}"
]

VANECK_PAGE_URL = (
    "https://www.vaneck.com/us/en/investments/"
    "pharmaceutical-etf-pph/holdings/"
)

VANECK_HOLDINGS_XLSX_URL = (
    "https://www.vaneck.com/us/en/investments/"
    "pharmaceutical-etf-pph/downloads/holdings/"
)

VANGUARD_URLS = [
    (
        "https://investor.vanguard.com/"
        "investment-products/etfs/profile/api/"
        "{t}/portfolio-holding/stock"
    )
]

CANONICAL_NAME_OVERRIDES: dict[str, str] = {}


class HoldingsError(RuntimeError):
    pass


@dataclass
class FetchResult:
    frame: pd.DataFrame
    source_date: str
    precision_method: str


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

    session.headers.update(
        HEADERS
    )

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
) -> requests.Response:

    response = session.get(
        url,
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


def try_urls(
    session: requests.Session,
    urls: Iterable[str],
    timeout: int,
) -> tuple[str, requests.Response]:

    errors: list[str] = []

    for url in urls:

        try:

            return (
                url,
                http_get(
                    session,
                    url,
                    timeout,
                ),
            )

        except Exception as exc:

            errors.append(
                f"{url}: {exc}"
            )

    raise HoldingsError(
        "All candidate URLs failed: "
        + " | ".join(errors)
    )


def utc_now() -> datetime:
    return datetime.now(
        timezone.utc
    )


def iso_today() -> str:
    return utc_now().date().isoformat()


def to_float(
    value: Any,
) -> float | None:

    if value is None or value is pd.NA:
        return None

    if (
        isinstance(
            value,
            float,
        )
        and
        pd.isna(value)
    ):
        return None

    text = str(
        value
    ).strip()

    if (
        not text
        or
        text.upper()
        in {
            "-",
            "--",
            "N/A",
            "NA",
            "NONE",
            "NAN",
        }
    ):
        return None

    negative = (
        text.startswith("(")
        and
        text.endswith(")")
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
            return -number

        return number

    except ValueError:
        return None


def pick_col(
    df: pd.DataFrame,
    candidates: list[str],
) -> str | None:

    columns = [
        (
            str(column),
            str(column)
            .strip()
            .lower(),
        )
        for column
        in df.columns
    ]

    for candidate in candidates:

        candidate_lower = (
            candidate.lower()
        )

        for original, lowered in columns:

            if candidate_lower in lowered:
                return original

    return None


def clean_text(
    value: Any,
) -> str:
    """
    Return plain text and recover complete tooltip names
    from old HTML fallback rows.
    """

    if value is None or value is pd.NA:
        return ""

    try:

        if pd.isna(value):
            return ""

    except Exception:
        pass

    text = str(
        value
    ).strip()

    if text.lower() in {
        "",
        "nan",
        "none",
        "null",
    }:
        return ""

    text = html_lib.unescape(
        text
    )

    title_match = re.search(
        r"""title\s*=\s*["']([^"']+)["']""",
        text,
        flags=re.IGNORECASE,
    )

    if title_match:

        text = (
            title_match
            .group(1)
        )

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


def normalize_date_value(
    value: Any,
) -> str | None:

    if value is None:
        return None

    if isinstance(
        value,
        (
            datetime,
            pd.Timestamp,
        ),
    ):
        return (
            value
            .date()
            .isoformat()
        )

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

    match = re.search(
        r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b",
        text,
    )

    if match:

        try:

            return date(
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
            ).isoformat()

        except ValueError:
            pass

    match = re.search(
        r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b",
        text,
    )

    if match:

        try:

            return date(
                int(match.group(3)),
                int(match.group(1)),
                int(match.group(2)),
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
        text[:10000],
    )

    for candidate in candidates:

        parsed = normalize_date_value(
            candidate
        )

        if parsed:
            return parsed

    return None


def recursive_find_date(
    payload: Any,
) -> str | None:

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

    lowercase_keys = {
        key.lower()
        for key
        in dateish_keys
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
                    str(key).lower()
                    in lowercase_keys
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


def sniff_csv(
    text: str,
    must_contain: list[str],
) -> tuple[pd.DataFrame, int]:

    lines = text.splitlines()

    header_index = None

    for index, line in enumerate(
        lines[:50]
    ):

        lowered = line.lower()

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
            f"Could not locate CSV header "
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
        clean_text(column)
        for column
        in frame.columns
    ]

    return (
        frame,
        header_index,
    )


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

    output[
        "raw_ticker"
    ] = (
        df[ticker]
        .map(clean_text)
        if ticker
        and ticker in df.columns
        else ""
    )

    output[
        "raw_name"
    ] = (
        df[name]
        .map(clean_text)
        if name
        and name in df.columns
        else ""
    )

    output[
        "weight"
    ] = (
        df[weight]
        .map(to_float)
        if weight
        and weight in df.columns
        else pd.NA
    )

    output[
        "shares"
    ] = (
        df[shares]
        .map(to_float)
        if shares
        and shares in df.columns
        else pd.NA
    )

    output[
        "market_value"
    ] = (
        df[mval]
        .map(to_float)
        if mval
        and mval in df.columns
        else pd.NA
    )

    output[
        "isin"
    ] = (
        df[isin]
        .map(clean_text)
        if isin
        and isin in df.columns
        else ""
    )

    output[
        "sedol"
    ] = (
        df[sedol]
        .map(clean_text)
        if sedol
        and sedol in df.columns
        else ""
    )

    output = output[
        (
            output[
                "raw_name"
            ].ne("")
            |
            output[
                "raw_ticker"
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
        output
        .reset_index(
            drop=True
        )[
            STD_COLS
        ]
    )


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

    if (
        ticker_upper.startswith("$")
        or
        "CASH"
        in ticker_upper
    ):
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


def maybe_recompute_from_market_value(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """
    Reconstruct high-precision weights from complete provider market values.

    Signed negative values are allowed for genuine cash/FX rows.
    This matters for ACWI because iShares can publish a negative
    USD cash balance.
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

    if not (
        market_value
        .notna()
        .all()
    ):

        return (
            output,
            "provider_weight",
        )

    negative_mask = (
        market_value < 0
    )

    if negative_mask.any():

        for index in (
            output.index[
                negative_mask
            ]
        ):

            ticker = clean_text(
                output.at[
                    index,
                    "raw_ticker",
                ]
            )

            name = clean_text(
                output.at[
                    index,
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

    total = float(
        market_value.sum()
    )

    if (
        not math.isfinite(total)
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

    ranked: list[
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

        if (
            not href
            or
            href.lower().startswith(
                (
                    "javascript:",
                    "mailto:",
                    "#",
                )
            )
        ):
            continue

        label = " ".join(
            clean_text(item)
            for item
            in anchor.itertext()
        ).strip().lower()

        href_lower = (
            href.lower()
        )

        text_ok = (
            not text_tokens
            or
            all(
                token in label
                for token
                in text_tokens
            )
        )

        href_ok = (
            not href_tokens
            or
            all(
                token
                in href_lower
                for token
                in href_tokens
            )
        )

        if not (
            text_ok
            and href_ok
        ):
            continue

        score = 0

        if "download" in label:
            score += 3

        if (
            "full holdings"
            in label
        ):
            score += 3

        if href_lower.endswith(
            (
                ".csv",
                ".xls",
                ".xlsx",
            )
        ):
            score += 3

        if (
            "holding"
            in href_lower
        ):
            score += 1

        ranked.append(
            (
                score,
                urljoin(
                    base_url,
                    href,
                ),
            )
        )

    if not ranked:
        return None

    ranked.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    return ranked[0][1]


def excel_with_detected_header(
    content: bytes,
    *,
    required_tokens: tuple[str, ...] = (
        "ticker",
    ),
    max_scan_rows: int = 40,
) -> tuple[pd.DataFrame, str | None]:

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

        joined = " | ".join(
            clean_text(value).lower()
            for value
            in raw.iloc[
                index
            ].tolist()
        )

        if all(
            token.lower()
            in joined
            for token
            in required_tokens
        ):

            header_index = index
            break

    if header_index is None:

        raise HoldingsError(
            f"Could not find Excel header "
            f"containing {required_tokens}"
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
            header_index + 1:
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


def parse_firsttrust_dom(
    html_text: str,
) -> pd.DataFrame:

    try:

        root = lxml_html.fromstring(
            html_text
        )

    except Exception as exc:

        raise HoldingsError(
            f"First Trust HTML parse failed: {exc}"
        ) from exc

    rows: list[
        dict[str, Any]
    ] = []

    for row in root.xpath(
        "//tr"
    ):

        cells = [
            " ".join(
                clean_text(item)
                for item
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

        if (
            not ticker
            or
            ticker.lower()
            in {
                "identifier",
                "ticker",
            }
        ):
            continue

        rows.append(
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

    if not rows:

        raise HoldingsError(
            "no First Trust holdings rows "
            "found in DOM"
        )

    return pd.DataFrame(
        rows,
        columns=STD_COLS,
    )


def parse_downloaded_holdings(
    response: requests.Response,
    *,
    provider: str,
    source_date_hint: str | None = None,
) -> tuple[pd.DataFrame, str | None]:

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

    if (
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
    ):

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
        in text[:2000]
    ):

        frame, _ = sniff_csv(
            text,
            ["ticker"],
        )

        return (
            frame,
            extract_date_from_text(
                text[:20000]
            )
            or source_date_hint,
        )

    raise HoldingsError(
        f"{provider}: download was neither "
        "recognizable CSV nor Excel "
        f"(content-type={content_type}, "
        f"final_url={response.url})"
    )


def fetch_ssga(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    url = SSGA_URL.format(
        t=etf.lower()
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
            20,
            len(raw),
        )
    ):

        values = " ".join(
            clean_text(value).lower()
            for value
            in raw.iloc[
                index
            ].tolist()
        )

        if (
            "ticker"
            in values
            and
            "weight"
            in values
        ):

            header_index = index
            break

    if header_index is None:

        raise HoldingsError(
            f"{etf}: SSGA holdings "
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
            header_index + 1:
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
        source_date
        or iso_today(),
        "provider_weight",
    )


def fetch_ishares(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    _, response = try_urls(
        session,
        ISHARES_ACWI_URLS,
        timeout,
    )

    text = response.text

    frame, _ = sniff_csv(
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

    if (
        precision
        !=
        "market_value_recomputed"
    ):

        raise HoldingsError(
            "ACWI: iShares market-value "
            "recomputation did not occur; "
            "refusing to publish rounded "
            "provider weights"
        )

    result = drop_nonsecurity_rows(
        result_all
    )

    return FetchResult(
        result,
        extract_date_from_text(
            text[:20000]
        )
        or iso_today(),
        precision,
    )


def fetch_invesco(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    source_date = iso_today()

    errors: list[str] = []

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
            page.status_code
            == 200
            and
            len(page.content)
            >= 100
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

    candidates.extend(
        url.format(
            t=etf
        )
        for url
        in INVESCO_URLS
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
                or
                len(response.content)
                < 100
            ):

                raise HoldingsError(
                    f"HTTP {response.status_code}, "
                    f"{len(response.content)} bytes"
                )

            parsed, download_date = (
                parse_downloaded_holdings(
                    response,
                    provider="Invesco",
                    source_date_hint=(
                        source_date
                    ),
                )
            )

            result_all = standardise(
                parsed,
                ticker=pick_col(
                    parsed,
                    [
                        "holding ticker",
                        "ticker",
                        "symbol",
                    ],
                ),
                name=pick_col(
                    parsed,
                    [
                        "name",
                        "security",
                        "holding",
                    ],
                ),
                weight=pick_col(
                    parsed,
                    [
                        "weight",
                        "% of fund",
                        "% of net",
                        "%",
                    ],
                ),
                shares=pick_col(
                    parsed,
                    [
                        "shares",
                        "quantity",
                    ],
                ),
                mval=pick_col(
                    parsed,
                    [
                        "market value",
                        "marketvalue",
                    ],
                ),
                isin=pick_col(
                    parsed,
                    [
                        "isin",
                        "security identifier",
                    ],
                ),
                sedol=pick_col(
                    parsed,
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


def fetch_globalx(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    page_url = (
        GLOBALX_PAGE_URL
        .format(
            t=etf.upper()
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

    csv_url = find_download_link(
        page.text,
        page.url,
        text_contains=(
            "full holdings",
        ),
        href_contains=(
            ".csv",
        ),
    )

    if not csv_url:

        try:

            root = (
                lxml_html
                .fromstring(
                    page.text
                )
            )

            links = []

            for anchor in root.xpath(
                "//a[@href]"
            ):

                href = clean_text(
                    anchor.get(
                        "href"
                    )
                )

                lowered = href.lower()

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

                links.append(
                    (
                        score,
                        urljoin(
                            page.url,
                            href,
                        ),
                    )
                )

            if links:

                links.sort(
                    key=lambda item:
                    item[0],
                    reverse=True,
                )

                csv_url = (
                    links[0][1]
                )

        except Exception:
            pass

    if not csv_url:

        raise HoldingsError(
            f"{etf}: Full Holdings "
            "CSV link not found"
        )

    response = http_get(
        session,
        csv_url,
        timeout,
    )

    parsed, download_date = (
        parse_downloaded_holdings(
            response,
            provider="Global X",
            source_date_hint=(
                source_date
            ),
        )
    )

    result_all = standardise(
        parsed,
        ticker=pick_col(
            parsed,
            ["ticker"],
        ),
        name=pick_col(
            parsed,
            ["name"],
        ),
        weight=pick_col(
            parsed,
            [
                "% of net assets",
                "net assets",
                "weight",
            ],
        ),
        shares=pick_col(
            parsed,
            [
                "shares held",
                "shares",
                "quantity",
            ],
        ),
        mval=pick_col(
            parsed,
            ["market value"],
        ),
        isin=pick_col(
            parsed,
            ["isin"],
        ),
        sedol=pick_col(
            parsed,
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


def fetch_firsttrust(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    urls = [
        url.format(
            t=etf
        )
        for url
        in FIRSTTRUST_URLS
    ]

    _, page = try_urls(
        session,
        urls,
        timeout,
    )

    source_date = (
        extract_date_from_text(
            page.text[:50000]
        )
        or iso_today()
    )

    errors: list[str] = []

    try:

        result_all = (
            parse_firsttrust_dom(
                page.text
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

        if (
            len(result)
            >=
            MIN_HOLDINGS_BY_TICKER[
                etf
            ]
        ):

            return FetchResult(
                result,
                source_date,
                precision,
            )

        errors.append(
            f"DOM parser returned "
            f"only {len(result)} rows"
        )

    except Exception as exc:

        errors.append(
            f"DOM parser: {exc}"
        )

    export_url = (
        find_download_link(
            page.text,
            page.url,
            text_contains=(
                "export",
                "excel",
            ),
        )
        or
        find_download_link(
            page.text,
            page.url,
            href_contains=(
                "excel",
            ),
        )
    )

    if export_url:

        try:

            response = http_get(
                session,
                export_url,
                timeout,
            )

            parsed, download_date = (
                parse_downloaded_holdings(
                    response,
                    provider="First Trust",
                    source_date_hint=(
                        source_date
                    ),
                )
            )

            result_all = standardise(
                parsed,
                ticker=pick_col(
                    parsed,
                    [
                        "identifier",
                        "ticker",
                        "symbol",
                    ],
                ),
                name=pick_col(
                    parsed,
                    [
                        "security name",
                        "security",
                        "name",
                    ],
                ),
                weight=pick_col(
                    parsed,
                    [
                        "weighting",
                        "weight",
                        "%",
                    ],
                ),
                shares=pick_col(
                    parsed,
                    [
                        "shares",
                        "quantity",
                    ],
                ),
                mval=pick_col(
                    parsed,
                    ["market value"],
                ),
                isin=pick_col(
                    parsed,
                    ["isin"],
                ),
                sedol=pick_col(
                    parsed,
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
                f"export parser: {exc}"
            )

    raise HoldingsError(
        f"{etf}: "
        + " | ".join(errors)
    )


def fetch_vaneck(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    source_date = iso_today()

    errors: list[str] = []

    try:

        response = session.get(
            VANECK_HOLDINGS_XLSX_URL,
            headers={
                **HEADERS,
                "Referer": (
                    VANECK_PAGE_URL
                ),
                "Accept": (
                    "application/vnd.openxmlformats-"
                    "officedocument.spreadsheetml.sheet,"
                    "application/vnd.ms-excel,"
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
            or
            len(response.content)
            < 100
        ):

            raise HoldingsError(
                f"HTTP {response.status_code}, "
                f"{len(response.content)} bytes"
            )

        parsed, download_date = (
            parse_downloaded_holdings(
                response,
                provider="VanEck",
                source_date_hint=(
                    source_date
                ),
            )
        )

        result_all = standardise(
            parsed,
            ticker=pick_col(
                parsed,
                [
                    "ticker",
                    "symbol",
                ],
            ),
            name=pick_col(
                parsed,
                [
                    "holding name",
                    "name",
                    "holding",
                    "security",
                ],
            ),
            weight=pick_col(
                parsed,
                [
                    "% of net assets",
                    "% of net",
                    "weight",
                ],
            ),
            shares=pick_col(
                parsed,
                [
                    "shares",
                    "quantity",
                ],
            ),
            mval=pick_col(
                parsed,
                ["market value"],
            ),
            isin=pick_col(
                parsed,
                ["isin"],
            ),
            sedol=pick_col(
                parsed,
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

        xls_url = (
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

        if not xls_url:

            raise HoldingsError(
                "Download XLS link not found"
            )

        response = http_get(
            session,
            xls_url,
            timeout,
        )

        parsed, download_date = (
            parse_downloaded_holdings(
                response,
                provider="VanEck",
                source_date_hint=(
                    source_date
                ),
            )
        )

        result_all = standardise(
            parsed,
            ticker=pick_col(
                parsed,
                [
                    "ticker",
                    "symbol",
                ],
            ),
            name=pick_col(
                parsed,
                [
                    "holding name",
                    "name",
                    "holding",
                    "security",
                ],
            ),
            weight=pick_col(
                parsed,
                [
                    "% of net assets",
                    "% of net",
                    "weight",
                ],
            ),
            shares=pick_col(
                parsed,
                [
                    "shares",
                    "quantity",
                ],
            ),
            mval=pick_col(
                parsed,
                ["market value"],
            ),
            isin=pick_col(
                parsed,
                ["isin"],
            ),
            sedol=pick_col(
                parsed,
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
            f"page discovery: {exc}"
        )

    raise HoldingsError(
        f"{etf}: "
        + " | ".join(errors)
    )


def _walk_dict_lists(
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

            yield from _walk_dict_lists(
                item
            )

    elif isinstance(
        value,
        dict,
    ):

        for item in (
            value.values()
        ):

            yield from _walk_dict_lists(
                item
            )


def _score_vanguard_list(
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

        if any(
            key in keys
            for key
            in {
                "ticker",
                "tickersymbol",
                "symbol",
            }
        ):
            score += 3

        if any(
            key in keys
            for key
            in {
                "sharesheld",
                "shares",
                "quantity",
            }
        ):
            score += 3

        if any(
            key in keys
            for key
            in {
                "marketvalue",
                "marketval",
            }
        ):
            score += 2

        if any(
            key in keys
            for key
            in {
                "longname",
                "shortname",
                "name",
            }
        ):
            score += 1

    return score


def fetch_vanguard(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:

    urls = [
        url.format(
            t=etf.lower()
        )
        for url
        in VANGUARD_URLS
    ]

    _, response = try_urls(
        session,
        urls,
        timeout,
    )

    try:

        payload = response.json()

    except json.JSONDecodeError as exc:

        raise HoldingsError(
            f"{etf}: Vanguard response "
            f"was not JSON: {exc}"
        ) from exc

    candidates = list(
        _walk_dict_lists(
            payload
        )
    )

    if not candidates:

        raise HoldingsError(
            f"{etf}: no holdings list "
            "found in Vanguard JSON"
        )

    entities = max(
        candidates,
        key=_score_vanguard_list,
    )

    if (
        _score_vanguard_list(
            entities
        )
        <= 0
    ):

        raise HoldingsError(
            f"{etf}: Vanguard JSON did "
            "not contain recognizable holdings"
        )

    rows: list[
        dict[str, Any]
    ] = []

    for entity in entities:

        rows.append(
            {
                "raw_ticker": clean_text(
                    entity.get("ticker")
                    or
                    entity.get(
                        "tickerSymbol"
                    )
                    or
                    entity.get("symbol")
                    or
                    ""
                ),
                "raw_name": clean_text(
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
                ),
                "weight": to_float(
                    entity.get(
                        "percentWeight"
                    )
                    or
                    entity.get("weight")
                    or
                    entity.get("percent")
                ),
                "shares": to_float(
                    entity.get(
                        "sharesHeld"
                    )
                    or
                    entity.get("shares")
                    or
                    entity.get(
                        "quantity"
                    )
                ),
                "market_value": to_float(
                    entity.get(
                        "marketValue"
                    )
                    or
                    entity.get(
                        "marketVal"
                    )
                    or
                    entity.get("value")
                ),
                "isin": clean_text(
                    entity.get("isin")
                    or
                    ""
                ),
                "sedol": clean_text(
                    entity.get("sedol")
                    or
                    ""
                ),
            }
        )

    result = pd.DataFrame(
        rows,
        columns=STD_COLS,
    )

    result = (
        drop_nonsecurity_rows(
            result
        )
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


def yahoo_ticker(
    raw_ticker: str,
) -> str:

    ticker = (
        clean_text(
            raw_ticker
        )
        .upper()
    )

    ticker = re.sub(
        r"\s+(US|UN|UW|UQ|UR|UF)$",
        "",
        ticker,
    )

    if re.fullmatch(
        r"[A-Z0-9]+\.([A-Z]{1,4})",
        ticker,
    ):

        suffix = (
            ticker
            .rsplit(
                ".",
                1,
            )[1]
        )

        if suffix in {
            "TW",
            "KS",
            "L",
            "PA",
            "AS",
            "DE",
            "SW",
            "TO",
            "HK",
            "T",
        }:

            return ticker

    return (
        ticker
        .replace(
            "/",
            "-",
        )
        .replace(
            ".",
            "-",
        )
    )


def _field_frame(
    data: pd.DataFrame,
    field: str,
    tickers: list[str],
) -> pd.DataFrame:

    if isinstance(
        data.columns,
        pd.MultiIndex,
    ):

        if (
            field
            not in
            data.columns
            .get_level_values(0)
        ):

            return pd.DataFrame(
                index=data.index
            )

        frame = data[field]

        if isinstance(
            frame,
            pd.Series,
        ):

            frame = (
                frame
                .to_frame(
                    tickers[0]
                    if tickers
                    else field
                )
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
            if len(tickers)
            == 1
            else field
        )

        return (
            series
            .to_frame(
                name
            )
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

    symbols = sorted(
        set(
            raw_to_yahoo
            .values()
        )
    )

    if not symbols:
        return {}

    try:

        data = yf.download(
            tickers=symbols,
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
            "Yahoo latest-price "
            f"download failed: {exc}"
        ) from exc

    if (
        data is None
        or
        len(data) == 0
    ):

        raise HoldingsError(
            "Yahoo returned no latest-price history"
        )

    closes = _field_frame(
        data,
        "Close",
        symbols,
    )

    yahoo_prices: dict[
        str,
        float,
    ] = {}

    for symbol in symbols:

        if symbol not in closes.columns:
            continue

        series = (
            pd.to_numeric(
                closes[symbol],
                errors="coerce",
            )
            .dropna()
        )

        if series.empty:
            continue

        price = float(
            series.iloc[-1]
        )

        if (
            math.isfinite(price)
            and
            price > 0
        ):

            yahoo_prices[
                symbol
            ] = price

    return {
        raw:
        yahoo_prices[
            symbol
        ]
        for raw, symbol
        in raw_to_yahoo.items()
        if symbol
        in yahoo_prices
    }


def fill_std_market_values_from_yahoo(
    frame: pd.DataFrame,
    *,
    label: str,
) -> pd.DataFrame:

    output = frame.copy()

    market_value = pd.to_numeric(
        output[
            "market_value"
        ],
        errors="coerce",
    )

    shares = pd.to_numeric(
        output[
            "shares"
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
            "raw_ticker"
        ]
        .astype(str)
        .ne("")
    )

    if not need.any():
        return output

    prices = yahoo_latest_prices(
        output.loc[
            need,
            "raw_ticker",
        ].map(
            clean_text
        )
    )

    filled = 0

    for index in (
        output.index[
            need
        ]
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


def drift_vgt_with_yahoo(
    df: pd.DataFrame,
    snapshot_date: str,
) -> pd.DataFrame:

    import yfinance as yf

    output = df.copy()

    if output.empty:

        raise HoldingsError(
            "VGT: Vanguard returned no holdings"
        )

    snapshot_date_value = (
        datetime
        .fromisoformat(
            snapshot_date
        )
        .date()
    )

    start = (
        snapshot_date_value
        - timedelta(days=3)
    ).isoformat()

    output[
        "_yf"
    ] = (
        output[
            "raw_ticker"
        ]
        .map(
            yahoo_ticker
        )
    )

    tickers = sorted(
        {
            ticker
            for ticker
            in output["_yf"]
            if ticker
        }
    )

    if not tickers:

        raise HoldingsError(
            "VGT: no Yahoo-priceable tickers"
        )

    print(
        f"[VGT] Pricing "
        f"{len(tickers)} securities "
        "via Yahoo...",
        file=sys.stderr,
    )

    try:

        history = yf.download(
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

        raise HoldingsError(
            "VGT: Yahoo bulk download "
            f"failed: {exc}"
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
        tickers,
    )

    splits = _field_frame(
        history,
        "Stock Splits",
        tickers,
    )

    latest_price: dict[
        str,
        float,
    ] = {}

    split_factor: dict[
        str,
        float,
    ] = {}

    for ticker in tickers:

        if ticker in closes.columns:

            series = (
                pd.to_numeric(
                    closes[ticker],
                    errors="coerce",
                )
                .dropna()
            )

            if not series.empty:

                latest_price[
                    ticker
                ] = float(
                    series.iloc[-1]
                )

        factor = 1.0

        if ticker in splits.columns:

            series = (
                pd.to_numeric(
                    splits[ticker],
                    errors="coerce",
                )
                .fillna(0.0)
            )

            for index, value in (
                series.items()
            ):

                index_date = (
                    pd.Timestamp(
                        index
                    )
                    .date()
                )

                if (
                    index_date
                    >
                    snapshot_date_value
                    and
                    value
                    not in (
                        0,
                        1,
                    )
                    and
                    value > 0
                ):

                    factor *= float(
                        value
                    )

        split_factor[
            ticker
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
        <
        len(output)
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

        ticker = row[
            "_yf"
        ]

        shares = to_float(
            row[
                "shares"
            ]
        )

        price = latest_price.get(
            ticker
        )

        factor = split_factor.get(
            ticker,
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

            today_shares = (
                shares
                * factor
            )

            live_values.append(
                today_shares
                * price
            )

            adjusted_shares.append(
                today_shares
            )

            continue

        market_value = to_float(
            row[
                "market_value"
            ]
        )

        row_weight = (
            float(
                snapshot_weight
                .iloc[index]
            )
            if
            index
            in snapshot_weight.index
            and
            pd.notna(
                snapshot_weight
                .iloc[index]
            )
            else
            0.0
        )

        fallback_weight += (
            max(
                row_weight,
                0.0,
            )
        )

        fallback_names.append(
            f"{row['raw_ticker']}"
            f"({row_weight:.4f}%)"
        )

        if (
            market_value is None
            or
            market_value < 0
        ):

            raise HoldingsError(
                "VGT: no live price and "
                "no snapshot market value "
                f"for {row['raw_ticker']} "
                f"{row['raw_name']}"
            )

        live_values.append(
            market_value
        )

        adjusted_shares.append(
            shares
        )

    if (
        fallback_weight
        >
        VGT_MAX_TOTAL_FALLBACK_WEIGHT_PCT
    ):

        raise HoldingsError(
            "VGT: unpriced holdings are "
            "too material to drift safely: "
            f"{fallback_weight:.4f}% > "
            f"{VGT_MAX_TOTAL_FALLBACK_WEIGHT_PCT:.4f}% "
            f"({', '.join(fallback_names[:10])})"
        )

    total_live = float(
        sum(
            live_values
        )
    )

    if total_live <= 0:

        raise HoldingsError(
            "VGT: zero total market value "
            "after Yahoo repricing"
        )

    output[
        "shares"
    ] = adjusted_shares

    output[
        "market_value"
    ] = live_values

    output[
        "weight"
    ] = (
        pd.Series(
            live_values,
            index=output.index,
        )
        / total_live
        * 100.0
    )

    output = output.drop(
        columns=[
            "_yf",
        ]
    )

    if fallback_names:

        print(
            f"[VGT] Tiny fallback tail "
            "retained at snapshot value: "
            f"{fallback_weight:.4f}%",
            file=sys.stderr,
        )

    return output


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
        clean_text(
            raw
        )
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
        clean_text(
            raw
        ),
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

    while (
        changed
        and
        text
    ):

        changed = False

        for pattern in (
            LEGAL_SUFFIX_PATTERNS
        ):

            new_text = re.sub(
                rf"(?:\s+{pattern})$",
                "",
                text,
            ).strip()

            if new_text != text:

                text = new_text
                changed = True

    return text


def valid_isin(
    value: str,
) -> bool:

    return bool(
        re.fullmatch(
            r"[A-Z]{2}[A-Z0-9]{9}[0-9]",
            clean_text(
                value
            ).upper(),
        )
    )


def valid_sedol(
    value: str,
) -> bool:

    return bool(
        re.fullmatch(
            r"[A-Z0-9]{7}",
            clean_text(
                value
            ).upper(),
        )
    )


def assign_identity_keys(
    long_df: pd.DataFrame,
) -> pd.DataFrame:

    frame = long_df.copy()

    frame[
        "_ticker"
    ] = (
        frame[
            "raw_ticker"
        ]
        .map(
            canonical_ticker
        )
    )

    frame[
        "_name_norm"
    ] = (
        frame[
            "raw_name"
        ]
        .map(
            normalized_name
        )
    )

    frame[
        "_isin"
    ] = (
        frame[
            "isin"
        ]
        .map(
            lambda value:
            clean_text(
                value
            ).upper()
        )
    )

    frame[
        "_sedol"
    ] = (
        frame[
            "sedol"
        ]
        .map(
            lambda value:
            clean_text(
                value
            ).upper()
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

        strong = None

        if valid_isin(
            row[
                "_isin"
            ]
        ):

            strong = (
                "ISIN:"
                + row[
                    "_isin"
                ]
            )

        elif valid_sedol(
            row[
                "_sedol"
            ]
        ):

            strong = (
                "SEDOL:"
                + row[
                    "_sedol"
                ]
            )

        if strong:

            (
                ticker_to_strong
                .setdefault(
                    ticker,
                    set(),
                )
                .add(
                    strong
                )
            )

    unique_ticker_alias = {
        ticker:
        next(
            iter(
                identifiers
            )
        )
        for ticker, identifiers
        in ticker_to_strong.items()
        if len(
            identifiers
        )
        == 1
    }

    def make_key(
        row: pd.Series,
    ) -> str:

        if valid_isin(
            row[
                "_isin"
            ]
        ):

            return (
                "ISIN:"
                + row[
                    "_isin"
                ]
            )

        if valid_sedol(
            row[
                "_sedol"
            ]
        ):

            return (
                "SEDOL:"
                + row[
                    "_sedol"
                ]
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
            + row[
                "_name_norm"
            ]
        )

    frame[
        "_identity"
    ] = (
        frame
        .apply(
            make_key,
            axis=1,
        )
    )

    return frame


def choose_canonical_name(
    group: pd.DataFrame,
) -> str:

    identity = str(
        group[
            "_identity"
        ].iloc[0]
    )

    tickers = [
        ticker
        for ticker
        in group[
            "_ticker"
        ]
        if ticker
    ]

    names = [
        clean_text(name)
        for name
        in group[
            "raw_name"
        ]
        if clean_text(
            name
        )
    ]

    if (
        identity
        in CANONICAL_NAME_OVERRIDES
    ):

        return (
            CANONICAL_NAME_OVERRIDES[
                identity
            ]
        )

    for ticker in tickers:

        key = f"TK:{ticker}"

        if (
            key
            in CANONICAL_NAME_OVERRIDES
        ):

            return (
                CANONICAL_NAME_OVERRIDES[
                    key
                ]
            )

    for name in names:

        key = normalized_name(
            name
        )

        if (
            key
            in CANONICAL_NAME_OVERRIDES
        ):

            return (
                CANONICAL_NAME_OVERRIDES[
                    key
                ]
            )

    if not names:

        if tickers:
            return tickers[0]

        return identity

    counts = (
        pd.Series(
            names
        )
        .value_counts()
    )

    top_count = int(
        counts.iloc[0]
    )

    tied = [
        name
        for name, count
        in counts.items()
        if int(count)
        == top_count
    ]

    def score(
        name: str,
    ):

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
        in group[
            "_ticker"
        ]
        if ticker
    ]

    if not tickers:
        return ""

    counts = (
        pd.Series(
            tickers
        )
        .value_counts()
    )

    top_count = int(
        counts.iloc[0]
    )

    tied = [
        ticker
        for ticker, count
        in counts.items()
        if int(count)
        == top_count
    ]

    qualified = [
        ticker
        for ticker
        in tied
        if "."
        in ticker
    ]

    if qualified:

        return sorted(
            qualified
        )[0]

    return sorted(
        tied,
        key=lambda value:
        (
            len(value),
            value,
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
    ] = (
        frame[
            "_identity"
        ]
        .map(
            lambda key:
            mapping[
                str(key)
            ][0]
        )
    )

    frame[
        "canonical_name"
    ] = (
        frame[
            "_identity"
        ]
        .map(
            lambda key:
            mapping[
                str(key)
            ][1]
        )
    )

    frame[
        "canonical_ticker"
    ] = (
        frame[
            "canonical_ticker"
        ]
        .where(
            frame[
                "canonical_ticker"
            ]
            != "",
            frame[
                "raw_ticker"
            ]
            .map(
                canonical_ticker
            ),
        )
    )

    frame[
        "canonical_name"
    ] = (
        frame[
            "canonical_name"
        ]
        .where(
            frame[
                "canonical_name"
            ]
            != "",
            frame[
                "raw_name"
            ]
            .map(
                clean_text
            ),
        )
    )

    return frame


def validate_live_frame(
    etf: str,
    df: pd.DataFrame,
    previous_count: int | None,
) -> None:

    if df.empty:

        raise HoldingsError(
            f"{etf}: no holdings"
        )

    hard_floor = (
        MIN_HOLDINGS_BY_TICKER[
            etf
        ]
    )

    required = hard_floor

    if (
        previous_count
        and
        previous_count
        >= hard_floor
    ):

        required = max(
            required,
            math.floor(
                previous_count
                * MIN_PREVIOUS_COUNT_RATIO
            ),
        )

    if len(df) < required:

        raise HoldingsError(
            f"{etf}: only {len(df)} rows; "
            f"required at least {required}"
        )

    weights = pd.to_numeric(
        df[
            "weight"
        ],
        errors="coerce",
    )

    numeric_count = int(
        weights
        .notna()
        .sum()
    )

    if (
        numeric_count
        <
        math.floor(
            len(df)
            * 0.98
        )
    ):

        raise HoldingsError(
            f"{etf}: only "
            f"{numeric_count}/{len(df)} "
            "rows have numeric weights"
        )

    if (
        weights
        .dropna()
        .lt(-0.01)
        .any()
    ):

        raise HoldingsError(
            f"{etf}: unexpected materially "
            "negative holding weight"
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
            f"{total:.6f}% outside "
            f"[{MIN_TOTAL_WEIGHT}, "
            f"{MAX_TOTAL_WEIGHT}]"
        )

    if (
        df[
            "raw_name"
        ]
        .astype(str)
        .str.contains(
            r"<[^>]+>",
            regex=True,
        )
        .any()
    ):

        raise HoldingsError(
            f"{etf}: HTML leaked "
            "into holding names"
        )

    duplicated = (
        df
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
                len(df)
                * 0.02
            ),
        )
    ):

        raise HoldingsError(
            f"{etf}: suspicious duplicate-row "
            f"count {int(duplicated.sum())}"
        )


def load_previous(
    combined_path: Path,
) -> dict[str, pd.DataFrame]:

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
            "WARNING: last-known-good "
            "schema mismatch; "
            "ignoring previous file",
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


def validate_previous_output(
    etf: str,
    frame: pd.DataFrame,
) -> None:

    if frame.empty:

        raise HoldingsError(
            f"{etf}: last-known-good "
            "rows empty"
        )

    floor = (
        MIN_HOLDINGS_BY_TICKER[
            etf
        ]
    )

    if len(frame) < floor:

        raise HoldingsError(
            f"{etf}: last-known-good "
            f"has only {len(frame)} "
            f"rows (<{floor})"
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
            f"{etf}: last-known-good "
            f"weight sum {total:.4f}% invalid"
        )


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
        output
        .loc[keep]
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
    recompute_weights: bool = False,
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

    filled = 0

    if need.any():

        prices = yahoo_latest_prices(
            output.loc[
                need,
                "holding_ticker",
            ]
            .map(
                clean_text
            )
        )

        for index in (
            output.index[
                need
            ]
        ):

            ticker = clean_text(
                output.at[
                    index,
                    "holding_ticker",
                ]
            )

            share_count = to_float(
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
                share_count is None
                or
                share_count <= 0
            ):
                continue

            output.at[
                index,
                "market_value_usd",
            ] = (
                share_count
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

    current_market_value = (
        pd.to_numeric(
            output[
                "market_value_usd"
            ],
            errors="coerce",
        )
    )

    if recompute_weights:

        if not (
            current_market_value
            .notna()
            .all()
        ):

            missing = int(
                current_market_value
                .isna()
                .sum()
            )

            raise HoldingsError(
                f"{label}: cannot recompute "
                "fallback weights; "
                f"{missing} market values "
                "remain blank"
            )

        total = float(
            current_market_value
            .sum()
        )

        if (
            not math.isfinite(total)
            or
            total <= 0
        ):

            raise HoldingsError(
                f"{label}: invalid fallback "
                "market-value denominator "
                f"{total}"
            )

        output[
            "weight"
        ] = (
            current_market_value
            / total
            * 100.0
        )

        output = (
            output
            .sort_values(
                "weight",
                ascending=False,
                na_position="last",
            )
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

    mode = (
        " + weight recompute"
        if recompute_weights
        else ""
    )

    print(
        f"[{label}] Fallback Yahoo "
        f"MV backfill{mode}: "
        f"{filled} filled, "
        f"{remaining} blank",
        file=sys.stderr,
    )

    return output


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
    )

    raise HoldingsError(
        "HTML/entity remnants survived "
        "holding-name cleanup: "
        f"{sample.to_dict(orient='records')}"
    )


def assert_high_precision_weights(
    frame: pd.DataFrame,
    fund_ticker: str,
    *,
    minimum_high_precision_rows: int,
) -> None:

    fund = (
        frame.loc[
            frame[
                "fund_ticker"
            ].eq(
                fund_ticker
            ),
            "weight",
        ]
    )

    weights = (
        pd.to_numeric(
            fund,
            errors="coerce",
        )
        .dropna()
    )

    if weights.empty:

        raise HoldingsError(
            f"{fund_ticker}: no numeric "
            "weights available for "
            "precision check"
        )

    beyond_2dp = (
        (
            weights
            - weights.round(2)
        )
        .abs()
        > 1e-10
    )

    count = int(
        beyond_2dp.sum()
    )

    if (
        count
        <
        minimum_high_precision_rows
    ):

        raise HoldingsError(
            f"{fund_ticker}: appears to "
            "have reverted to rounded "
            "2dp weights; "
            f"only {count} rows contain "
            "precision beyond 2 decimals"
        )


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
            ]
            == etf
        ]
        .copy()
    )

    fund[
        "weight"
    ] = (
        pd.to_numeric(
            fund[
                "weight"
            ],
            errors="coerce",
        )
    )

    fund[
        "shares"
    ] = (
        pd.to_numeric(
            fund[
                "shares"
            ],
            errors="coerce",
        )
    )

    fund[
        "market_value"
    ] = (
        pd.to_numeric(
            fund[
                "market_value"
            ],
            errors="coerce",
        )
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
            "fund_ticker": (
                etf
            ),
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


def write_csv_atomic(
    frame: pd.DataFrame,
    destination: Path,
) -> None:

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = (
        destination
        .with_name(
            f".{destination.name}.tmp"
        )
    )

    try:

        frame.to_csv(
            temporary,
            index=False,
            encoding="utf-8-sig",
            float_format="%.15g",
        )

        if (
            not temporary.exists()
            or
            temporary.stat().st_size
            == 0
        ):

            raise HoldingsError(
                f"Generated empty CSV: "
                f"{temporary}"
            )

        os.replace(
            temporary,
            destination,
        )

    finally:

        if temporary.exists():

            temporary.unlink()


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


def fetch_all_live(
    session: requests.Session,
    etfs: list[str],
    previous: dict[
        str,
        pd.DataFrame,
    ],
    timeout: int,
) -> tuple[
    dict[
        str,
        pd.DataFrame,
    ],
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

        print(
            f"[{etf}] Fetching "
            f"{PROVIDER_LABELS[etf]}...",
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

            frame[
                "etf"
            ] = etf

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

            print(
                f"[{etf}] OK: "
                f"{len(frame)} rows, "
                f"{frame['weight'].sum(skipna=True):.8f}% "
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
                if etf
                in live
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

            validate_previous_output(
                etf,
                output,
            )

            final_by_etf[
                etf
            ] = output

            source_status[
                etf
            ] = (
                f"{PROVIDER_LABELS[etf]} / "
                f"{precision_methods[etf]}"
            )

            continue

        if etf in previous:

            try:

                previous_output = (
                    sanitize_previous_output(
                        previous[
                            etf
                        ].copy()
                    )
                )

                validate_previous_output(
                    etf,
                    previous_output,
                )

                previous_output = (
                    fill_output_market_values_from_yahoo(
                        previous_output,
                        label=etf,
                        retrieved_at=(
                            retrieved_at
                        ),
                        recompute_weights=(
                            etf == "SOXQ"
                        ),
                    )
                )

                assert_no_html_remnants(
                    previous_output
                )

                validate_previous_output(
                    etf,
                    previous_output,
                )

                final_by_etf[
                    etf
                ] = (
                    previous_output[
                        OUTPUT_COLS
                    ]
                )

                source_status[
                    etf
                ] = (
                    "Last-known-good / "
                    "cleaned + MV backfill"
                    +
                    (
                        " + weight recompute"
                        if etf
                        == "SOXQ"
                        else ""
                    )
                )

                print(
                    f"[{etf}] Using cleaned "
                    "last-known-good rows; "
                    "source_date preserved",
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

    if (
        actual_funds
        !=
        expected_funds
    ):

        print(
            "ERROR: combined ETF set mismatch. "
            f"Expected={expected_funds}, "
            f"Actual={actual_funds}",
            file=sys.stderr,
        )

        return 1

    if (
        list(
            combined.columns
        )
        !=
        OUTPUT_COLS
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

        if (
            "ACWI"
            in expected_funds
        ):

            assert_high_precision_weights(
                combined,
                "ACWI",
                minimum_high_precision_rows=100,
            )

        if (
            "SOXQ"
            in expected_funds
        ):

            assert_high_precision_weights(
                combined,
                "SOXQ",
                minimum_high_precision_rows=10,
            )

        for etf, group in (
            combined.groupby(
                "fund_ticker",
                sort=False,
            )
        ):

            validate_previous_output(
                str(etf),
                group,
            )

    except Exception as exc:

        print(
            "ERROR: final validation "
            f"failed: {exc}",
            file=sys.stderr,
        )

        return 1

    market_value = (
        pd.to_numeric(
            combined[
                "market_value_usd"
            ],
            errors="coerce",
        )
    )

    if market_value.isna().any():

        print(
            "\nMarket-value blanks "
            "remaining by fund:",
            file=sys.stderr,
        )

        counts = (
            combined.loc[
                market_value.isna()
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
                f"  - {fund}: {count}",
                file=sys.stderr,
            )

    else:

        print(
            "\nMarket-value coverage: "
            "100.00%",
            file=sys.stderr,
        )

    write_csv_atomic(
        combined,
        combined_path,
    )

    print(
        f"\nPublished "
        f"{len(combined):,} rows "
        f"across {len(etfs)} ETFs -> "
        f"{combined_path}",
        file=sys.stderr,
    )

    print(
        "Sources used:",
        file=sys.stderr,
    )

    for etf in etfs:

        print(
            f"  - {etf}: "
            f"{source_status[etf]}",
            file=sys.stderr,
        )

    return 0


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "tickers",
        nargs="*",
        help=(
            "Optional ETF tickers. "
            "Defaults to the existing "
            "9-fund basket."
        ),
    )

    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
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
        !=
        args.combined_name
    ):

        print(
            "ERROR: --combined-name "
            "must be a filename, "
            "not a path",
            file=sys.stderr,
        )

        return 2

    if args.timeout <= 0:

        print(
            "ERROR: --timeout "
            "must be > 0",
            file=sys.stderr,
        )

        return 2

    etfs = (
        [
            ticker
            .strip()
            .upper()
            for ticker
            in args.tickers
            if ticker.strip()
        ]
        if args.tickers
        else DEFAULT_ETFS.copy()
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
            f"ERROR: unknown ETF(s): "
            f"{unknown}. "
            f"Known: {list(FETCHERS)}",
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
