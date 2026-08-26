"""
Standalone ETF holdings updater.

Usage:
    python update_holdings.py \
        --out-dir data \
        --combined-name ETF_Holdings_Latest.csv \
        VGT ACWI XLF XLI XLC PPH MLPX GRID SOXQ

Dependencies:
    pip install pandas requests yfinance openpyxl lxml html5lib curl_cffi

Important:
- This file is fully standalone.
- It does NOT import update_holdings_base.
- It does NOT download/exec Python code at runtime.
- Output schema is kept stable for Power Query.
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


DEFAULT_OUT_DIR = Path("data")
DEFAULT_COMBINED_NAME = "ETF_Holdings_Latest.csv"
DEFAULT_TIMEOUT_SECONDS = 30

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

HTML_ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,*/*;q=0.8"
)

DOWNLOAD_ACCEPT = (
    "text/csv,application/csv,application/vnd.ms-excel,"
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
    "application/octet-stream;q=0.9,*/*;q=0.8"
)

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

MIN_HOLDINGS = {
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

MIN_PREVIOUS_RATIO = 0.90
MIN_TOTAL_WEIGHT = 97.0
MAX_TOTAL_WEIGHT = 103.0

VGT_WARN_DRIFT_DAYS = 10
VGT_MAX_DRIFT_DAYS = 45
SOXQ_WARN_DRIFT_DAYS = 7
SOXQ_MAX_DRIFT_DAYS = 30

MAX_UNPRICED_FALLBACK_WEIGHT = {
    "VGT": 0.10,
    "SOXQ": 0.10,
}

SSGA_URL = (
    "https://www.ssga.com/us/en/intermediary/etfs/"
    "library-content/products/fund-data/etfs/us/"
    "holdings-daily-us-en-{ticker}.xlsx"
)

ISHARES_ACWI_URL = (
    "https://www.ishares.com/us/products/239600/"
    "ishares-msci-acwi-etf/latest-holdings.csv"
)

VANGUARD_URL = (
    "https://investor.vanguard.com/"
    "investment-products/etfs/profile/api/"
    "{ticker}/portfolio-holding/stock"
)

INVESCO_PAGE_URL = (
    "https://www.invesco.com/us/en/financial-products/"
    "etfs/invesco-phlx-semiconductor-etf.html"
)

INVESCO_EXPORT_URLS = [
    (
        "https://www.invesco.com/us/financial-products/"
        "etfs/holdings/main/holdings/0"
        "?audienceType=Investor&action=download&ticker={ticker}"
    ),
    (
        "https://www.invesco.com/us/en/financial-products/"
        "etfs/holdings/main/holdings/0"
        "?audienceType=Investor&action=download&ticker={ticker}"
    ),
]

GLOBALX_PAGE_URL = "https://www.globalxetfs.com/funds/{ticker}"

FIRSTTRUST_URL = (
    "https://www.ftportfolios.com/Retail/Etf/"
    "EtfHoldings.aspx?Ticker={ticker}"
)

VANECK_PAGE_URL = (
    "https://www.vaneck.com/us/en/investments/"
    "pharmaceutical-etf-pph/holdings/"
)

VANECK_XLSX_URL = (
    "https://www.vaneck.com/us/en/investments/"
    "pharmaceutical-etf-pph/downloads/holdings/"
)

ISHARES_EXCHANGE_SUFFIX = {
    "Taiwan Stock Exchange": ".TW",
    "Korea Exchange (Stock Market)": ".KS",
    "Korea Exchange (Kosdaq)": ".KQ",
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


class HoldingsError(RuntimeError):
    pass


@dataclass
class FetchResult:
    frame: pd.DataFrame
    source_date: str
    precision_method: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_today() -> str:
    return utc_now().date().isoformat()


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def clean_text(value: Any) -> str:
    if value is None or value is pd.NA:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    text = html_lib.unescape(str(value).strip())
    if text.lower() in {"", "nan", "none", "null"}:
        return ""

    title_match = re.search(
        r"""title\s*=\s*["']([^"']+)["']""",
        text,
        flags=re.IGNORECASE,
    )
    if title_match:
        text = title_match.group(1)
    else:
        text = re.sub(r"<[^>]+>", " ", text)

    return re.sub(r"\s+", " ", html_lib.unescape(text)).strip()


def to_float(value: Any) -> float | None:
    if value is None or value is pd.NA:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = str(value).strip()
    if not text or text.upper() in {"-", "--", "N/A", "NA", "NONE", "NAN"}:
        return None

    negative = text.startswith("(") and text.endswith(")")
    text = (
        text.replace(",", "")
        .replace("%", "")
        .replace("$", "")
        .strip("() ")
    )
    text = re.sub(r"[^0-9eE+\-.]", "", text)
    if not text:
        return None

    try:
        value_float = float(text)
    except ValueError:
        return None

    return -value_float if negative else value_float


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

    for pattern, fmt in (
        (r"\b20\d{2}-\d{1,2}-\d{1,2}\b", "%Y-%m-%d"),
        (r"\b\d{1,2}/\d{1,2}/20\d{2}\b", "%m/%d/%Y"),
    ):
        match = re.search(pattern, text)
        if match:
            try:
                return datetime.strptime(match.group(0), fmt).date().isoformat()
            except ValueError:
                pass

    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass

    return None


def extract_date_from_text(text: str) -> str | None:
    candidates = re.findall(
        r"\b(?:"
        r"20\d{2}-\d{1,2}-\d{1,2}"
        r"|"
        r"\d{1,2}/\d{1,2}/20\d{2}"
        r"|"
        r"[A-Za-z]+\s+\d{1,2},?\s+20\d{2}"
        r")\b",
        text[:50000],
    )

    for candidate in candidates:
        parsed = normalize_date_value(candidate)
        if parsed:
            return parsed

    return None


def recursive_find_date(payload: Any) -> str | None:
    keys = {
        "asofdate",
        "as_of_date",
        "asof",
        "effectivedate",
        "effective_date",
        "portfoliodate",
        "portfolio_date",
        "date",
    }

    stack = [payload]

    while stack:
        item = stack.pop()

        if isinstance(item, dict):
            for key, value in item.items():
                if str(key).lower() in keys:
                    parsed = normalize_date_value(value)
                    if parsed:
                        return parsed
            stack.extend(item.values())

        elif isinstance(item, list):
            stack.extend(item)

    return None


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    columns = [(str(col), str(col).strip().lower()) for col in df.columns]

    for candidate in candidates:
        candidate_lower = candidate.lower()

        for original, lowered in columns:
            if candidate_lower in lowered:
                return original

    return None


def _series(
    df: pd.DataFrame,
    column: str | None,
    mapper: Callable[[Any], Any],
    default: Any,
) -> pd.Series:
    if column and column in df.columns:
        return df[column].map(mapper)

    return pd.Series([default] * len(df), index=df.index)


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

    out["raw_ticker"] = _series(df, ticker, clean_text, "")
    out["raw_name"] = _series(df, name, clean_text, "")
    out["weight"] = _series(df, weight, to_float, pd.NA)
    out["shares"] = _series(df, shares, to_float, pd.NA)
    out["market_value"] = _series(df, mval, to_float, pd.NA)
    out["isin"] = _series(df, isin, clean_text, "")
    out["sedol"] = _series(df, sedol, clean_text, "")

    out = out[
        out["raw_name"].ne("")
        | out["raw_ticker"].ne("")
    ].copy()

    out["isin"] = out["isin"].astype(str).str.strip().str.upper()
    out["sedol"] = out["sedol"].astype(str).str.strip().str.upper()

    return out.reset_index(drop=True)[STD_COLS]


def is_nonsecurity(ticker: str, name: str) -> bool:
    ticker_upper = clean_text(ticker).upper().strip()
    name_upper = clean_text(name).upper().strip()

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

    if ticker_upper.startswith("$") or "CASH" in ticker_upper:
        return True

    nonsecurity_tokens = (
        "US DOLLAR",
        "USD CASH",
        "OTHER/CASH",
        "SECURITIES LENDING",
        "GOVERNMENT & AGENCY PORTFOLIO",
    )
    if any(token in name_upper for token in nonsecurity_tokens):
        return True

    # Keep actual listed securities, but remove obvious derivatives/cash lines.
    if any(
        token in name_upper
        for token in ("FUTURE", "FUTURES", "FX FORWARD", "FORWARD CONTRACT")
    ):
        return True

    return False


def drop_nonsecurity_rows(df: pd.DataFrame) -> pd.DataFrame:
    keep = [
        not is_nonsecurity(ticker, name)
        for ticker, name in zip(df["raw_ticker"], df["raw_name"])
    ]

    return df.loc[keep].reset_index(drop=True)


def http_get(
    session: requests.Session,
    url: str,
    timeout: int,
    *,
    headers: dict[str, str] | None = None,
) -> requests.Response:
    response = session.get(
        url,
        headers=headers,
        timeout=(10, timeout),
        allow_redirects=True,
    )

    if response.status_code != 200:
        raise HoldingsError(f"HTTP {response.status_code}: {url}")

    if len(response.content) < 100:
        raise HoldingsError(
            f"Suspiciously small response ({len(response.content)} bytes): {url}"
        )

    return response


def sniff_csv(text: str, required: list[str]) -> pd.DataFrame:
    lines = text.splitlines()
    header_index = None

    for index, line in enumerate(lines[:80]):
        lowered = line.lower()

        if all(token.lower() in lowered for token in required):
            header_index = index
            break

    if header_index is None:
        raise HoldingsError(
            f"CSV header containing {required} not found. "
            f"First lines={lines[:5]}"
        )

    frame = pd.read_csv(
        io.StringIO("\n".join(lines[header_index:])),
        dtype=str,
        low_memory=False,
    )

    frame.columns = [clean_text(column) for column in frame.columns]
    return frame


def excel_with_detected_header(
    content: bytes,
    *,
    required_tokens: tuple[str, ...] = ("ticker",),
) -> tuple[pd.DataFrame, str | None]:
    raw = pd.read_excel(
        io.BytesIO(content),
        header=None,
        dtype=str,
    )

    header_index = None

    for index in range(min(50, len(raw))):
        row_text = " | ".join(
            clean_text(value).lower()
            for value in raw.iloc[index].tolist()
        )

        if all(token.lower() in row_text for token in required_tokens):
            header_index = index
            break

    if header_index is None:
        raise HoldingsError(
            f"Excel header containing {required_tokens} not found"
        )

    source_date = None

    for index in range(header_index):
        row_text = " ".join(
            clean_text(value)
            for value in raw.iloc[index].tolist()
        )

        parsed = extract_date_from_text(row_text)

        if parsed:
            source_date = parsed
            break

    frame = raw.iloc[header_index + 1 :].copy()
    frame.columns = [
        clean_text(column)
        for column in raw.iloc[header_index].tolist()
    ]

    return frame, source_date


def parse_download(
    response: Any,
    *,
    provider: str,
    source_date_hint: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    content_type = str(
        response.headers.get("content-type", "")
    ).lower()

    final_url = str(response.url).lower()
    content = response.content

    if (
        "spreadsheet" in content_type
        or "excel" in content_type
        or final_url.endswith((".xls", ".xlsx"))
        or content[:2] == b"PK"
    ):
        frame, source_date = excel_with_detected_header(
            content,
            required_tokens=("ticker",),
        )

        return frame, source_date or source_date_hint

    text = response.text

    if "ticker" in text[:15000].lower() and "," in text[:3000]:
        return (
            sniff_csv(text, ["ticker"]),
            extract_date_from_text(text[:30000]) or source_date_hint,
        )

    raise HoldingsError(
        f"{provider}: download was not recognizable CSV/Excel "
        f"(content-type={content_type}, url={response.url})"
    )


def find_download_link(
    html_text: str,
    base_url: str,
    *,
    text_contains: tuple[str, ...] = (),
    href_contains: tuple[str, ...] = (),
) -> str | None:
    try:
        root = lxml_html.fromstring(html_text)
    except Exception:
        return None

    ranked: list[tuple[int, str]] = []

    for anchor in root.xpath("//a[@href]"):
        href = clean_text(anchor.get("href"))

        if not href or href.lower().startswith(
            ("javascript:", "mailto:", "#")
        ):
            continue

        label = " ".join(
            clean_text(item)
            for item in anchor.itertext()
        ).lower()

        href_lower = href.lower()

        if text_contains and not all(
            token.lower() in label
            for token in text_contains
        ):
            continue

        if href_contains and not all(
            token.lower() in href_lower
            for token in href_contains
        ):
            continue

        score = 0

        if "download" in label or "export" in label:
            score += 3

        if "full holdings" in label:
            score += 3

        if href_lower.endswith((".csv", ".xls", ".xlsx")):
            score += 3

        if "holding" in href_lower:
            score += 2

        ranked.append(
            (
                score,
                urljoin(base_url, href),
            )
        )

    if not ranked:
        return None

    ranked.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    return ranked[0][1]


def qualify_ishares_ticker(
    raw_ticker: Any,
    exchange: Any,
) -> str:
    ticker = clean_text(raw_ticker)
    exchange_name = clean_text(exchange)

    if not ticker:
        return ""

    suffix = ISHARES_EXCHANGE_SUFFIX.get(exchange_name)

    if suffix and "." not in ticker:
        return ticker + suffix

    return ticker


def recompute_weights(
    frame: pd.DataFrame,
    *,
    require_all_market_values: bool,
) -> tuple[pd.DataFrame, str]:
    out = frame.copy()

    market_value = pd.to_numeric(
        out["market_value"],
        errors="coerce",
    )

    if require_all_market_values and not market_value.notna().all():
        return out, "provider_weight"

    valid = market_value.notna()

    if not valid.any():
        return out, "provider_weight"

    total = float(
        market_value.loc[valid].sum()
    )

    if not math.isfinite(total) or total <= 0:
        return out, "provider_weight"

    out.loc[valid, "weight"] = (
        market_value.loc[valid]
        / total
        * 100.0
    )

    return out, "market_value_recomputed"


def yahoo_ticker(raw_ticker: str) -> str:
    ticker = clean_text(raw_ticker).upper()

    ticker = re.sub(
        r"\s+(US|UN|UW|UQ|UR|UF)$",
        "",
        ticker,
    )

    # Already exchange qualified.
    if re.fullmatch(
        r"[A-Z0-9\-]+\.[A-Z]{1,4}",
        ticker,
    ):
        return ticker

    return (
        ticker
        .replace("/", "-")
        .replace(".", "-")
    )


def _yf_field(
    data: pd.DataFrame,
    field: str,
    symbols: list[str],
) -> pd.DataFrame:
    if isinstance(data.columns, pd.MultiIndex):
        if field not in data.columns.get_level_values(0):
            return pd.DataFrame(index=data.index)

        result = data[field]

        if isinstance(result, pd.Series):
            name = symbols[0] if symbols else field
            return result.to_frame(name)

        return result

    if field not in data.columns:
        return pd.DataFrame(index=data.index)

    result = data[field]

    if isinstance(result, pd.Series):
        name = symbols[0] if len(symbols) == 1 else field
        return result.to_frame(name)

    return result


def yahoo_prices_and_splits(
    raw_tickers: Iterable[str],
    since_date: date,
) -> tuple[dict[str, float], dict[str, float]]:
    import yfinance as yf

    raw_to_yahoo: dict[str, str] = {}

    for raw in raw_tickers:
        raw_clean = clean_text(raw)

        if not raw_clean:
            continue

        converted = yahoo_ticker(raw_clean)

        if converted:
            raw_to_yahoo[raw_clean] = converted

    symbols = sorted(
        set(raw_to_yahoo.values())
    )

    if not symbols:
        return {}, {}

    data = yf.download(
        tickers=symbols,
        start=(since_date - timedelta(days=3)).isoformat(),
        interval="1d",
        actions=True,
        auto_adjust=False,
        keepna=False,
        progress=False,
        threads=True,
        timeout=20,
        group_by="column",
    )

    if data is None or len(data) == 0:
        raise HoldingsError(
            "Yahoo returned no price/split history"
        )

    closes = _yf_field(
        data,
        "Close",
        symbols,
    )

    splits = _yf_field(
        data,
        "Stock Splits",
        symbols,
    )

    yahoo_prices: dict[str, float] = {}
    yahoo_split_factors: dict[str, float] = {
        symbol: 1.0
        for symbol in symbols
    }

    for symbol in symbols:
        if symbol in closes.columns:
            series = pd.to_numeric(
                closes[symbol],
                errors="coerce",
            ).dropna()

            if not series.empty:
                price = float(
                    series.iloc[-1]
                )

                if math.isfinite(price) and price > 0:
                    yahoo_prices[symbol] = price

        if symbol in splits.columns:
            series = pd.to_numeric(
                splits[symbol],
                errors="coerce",
            ).fillna(0.0)

            factor = 1.0

            for index, value in series.items():
                index_date = pd.Timestamp(index).date()

                if (
                    index_date > since_date
                    and value not in (0, 1)
                    and value > 0
                ):
                    factor *= float(value)

            yahoo_split_factors[symbol] = factor

    prices = {
        raw: yahoo_prices[symbol]
        for raw, symbol in raw_to_yahoo.items()
        if symbol in yahoo_prices
    }

    split_factors = {
        raw: yahoo_split_factors.get(symbol, 1.0)
        for raw, symbol in raw_to_yahoo.items()
    }

    return prices, split_factors


def yahoo_latest_prices(
    raw_tickers: Iterable[str],
) -> dict[str, float]:
    import yfinance as yf

    raw_to_yahoo: dict[str, str] = {}

    for raw in raw_tickers:
        raw_clean = clean_text(raw)

        if not raw_clean:
            continue

        converted = yahoo_ticker(raw_clean)

        if converted:
            raw_to_yahoo[raw_clean] = converted

    symbols = sorted(
        set(raw_to_yahoo.values())
    )

    if not symbols:
        return {}

    data = yf.download(
        tickers=symbols,
        period="5d",
        interval="1d",
        actions=False,
        auto_adjust=False,
        keepna=False,
        progress=False,
        threads=True,
        timeout=20,
        group_by="column",
    )

    if data is None or len(data) == 0:
        raise HoldingsError(
            "Yahoo returned no latest-price history"
        )

    closes = _yf_field(
        data,
        "Close",
        symbols,
    )

    yahoo_prices: dict[str, float] = {}

    for symbol in symbols:
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

        if math.isfinite(price) and price > 0:
            yahoo_prices[symbol] = price

    return {
        raw: yahoo_prices[symbol]
        for raw, symbol in raw_to_yahoo.items()
        if symbol in yahoo_prices
    }


def _walk_dict_lists(value: Any):
    if isinstance(value, list):
        if value and all(
            isinstance(item, dict)
            for item in value
        ):
            yield value

        for item in value:
            yield from _walk_dict_lists(item)

    elif isinstance(value, dict):
        for item in value.values():
            yield from _walk_dict_lists(item)


def _score_holdings_list(
    rows: list[dict[str, Any]],
) -> int:
    score = 0

    for row in rows[:20]:
        keys = {
            str(key).lower()
            for key in row
        }

        if {
            "ticker",
            "tickersymbol",
            "symbol",
        } & keys:
            score += 3

        if {
            "sharesheld",
            "shares",
            "quantity",
        } & keys:
            score += 3

        if {
            "marketvalue",
            "marketval",
        } & keys:
            score += 2

        if {
            "longname",
            "shortname",
            "name",
            "securityname",
        } & keys:
            score += 1

    return score


def parse_vanguard_payload(
    payload: Any,
    etf: str,
) -> FetchResult:
    candidates = list(
        _walk_dict_lists(payload)
    )

    if not candidates:
        raise HoldingsError(
            f"{etf}: no holdings list in Vanguard JSON"
        )

    entities = max(
        candidates,
        key=_score_holdings_list,
    )

    if _score_holdings_list(entities) <= 0:
        raise HoldingsError(
            f"{etf}: Vanguard JSON had no recognizable holdings"
        )

    rows: list[dict[str, Any]] = []

    for entity in entities:
        rows.append(
            {
                "raw_ticker": clean_text(
                    entity.get("ticker")
                    or entity.get("tickerSymbol")
                    or entity.get("symbol")
                    or ""
                ),
                "raw_name": clean_text(
                    entity.get("longName")
                    or entity.get("shortName")
                    or entity.get("name")
                    or entity.get("securityName")
                    or ""
                ),
                "weight": to_float(
                    entity.get("percentWeight")
                    or entity.get("weight")
                    or entity.get("percent")
                ),
                "shares": to_float(
                    entity.get("sharesHeld")
                    or entity.get("shares")
                    or entity.get("quantity")
                ),
                "market_value": to_float(
                    entity.get("marketValue")
                    or entity.get("marketVal")
                    or entity.get("value")
                ),
                "isin": clean_text(
                    entity.get("isin")
                    or ""
                ),
                "sedol": clean_text(
                    entity.get("sedol")
                    or ""
                ),
            }
        )

    frame = pd.DataFrame(
        rows,
        columns=STD_COLS,
    )

    frame = drop_nonsecurity_rows(frame)

    return FetchResult(
        frame=frame,
        source_date=(
            recursive_find_date(payload)
            or iso_today()
        ),
        precision_method="vanguard_snapshot",
    )


def fetch_vanguard(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:
    url = VANGUARD_URL.format(
        ticker=etf.lower()
    )

    errors: list[str] = []

    try:
        response = session.get(
            url,
            headers={
                **HEADERS,
                "Accept": "application/json,text/plain,*/*",
                "Referer": (
                    "https://investor.vanguard.com/"
                    f"investment-products/etfs/profile/{etf.lower()}"
                ),
            },
            timeout=(10, timeout),
            allow_redirects=True,
        )

        if (
            response.status_code == 200
            and len(response.content) >= 100
        ):
            try:
                return parse_vanguard_payload(
                    response.json(),
                    etf,
                )
            except Exception as exc:
                errors.append(
                    f"requests JSON parse: {exc}"
                )

        else:
            errors.append(
                f"requests HTTP {response.status_code}, "
                f"{len(response.content)} bytes"
            )

    except Exception as exc:
        errors.append(
            f"requests: {type(exc).__name__}: {exc}"
        )

    print(
        f"[{etf}] Vanguard requests path failed; "
        "retrying with curl_cffi...",
        file=sys.stderr,
    )

    try:
        from curl_cffi import requests as cffi_requests

        browser = cffi_requests.Session(
            impersonate="chrome",
            headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
            },
        )

        try:
            response = browser.get(
                url,
                headers={
                    "Accept": "application/json,text/plain,*/*",
                    "Referer": (
                        "https://investor.vanguard.com/"
                        f"investment-products/etfs/profile/{etf.lower()}"
                    ),
                },
                timeout=timeout,
                allow_redirects=True,
            )

            if (
                response.status_code == 200
                and len(response.content) >= 100
            ):
                return parse_vanguard_payload(
                    response.json(),
                    etf,
                )

            errors.append(
                f"curl_cffi HTTP {response.status_code}, "
                f"{len(response.content)} bytes"
            )

        finally:
            try:
                browser.close()
            except Exception:
                pass

    except Exception as exc:
        errors.append(
            f"curl_cffi: {type(exc).__name__}: {exc}"
        )

    raise HoldingsError(
        f"{etf}: Vanguard unavailable: "
        + " | ".join(errors[-5:])
    )


def drift_vgt_live_snapshot(
    frame: pd.DataFrame,
    source_date: str,
) -> pd.DataFrame:
    out = frame.copy()

    if out.empty:
        raise HoldingsError(
            "VGT: Vanguard returned no holdings"
        )

    snapshot_date = datetime.fromisoformat(
        source_date
    ).date()

    tickers = out["raw_ticker"].map(
        clean_text
    )

    prices, split_factors = yahoo_prices_and_splits(
        tickers,
        snapshot_date,
    )

    old_weight = pd.to_numeric(
        out["weight"],
        errors="coerce",
    ).fillna(0.0)

    market_values: list[float] = []
    adjusted_shares: list[float | None] = []

    missing_weight = 0.0
    missing_names: list[str] = []

    for index, row in out.iterrows():
        ticker = clean_text(
            row["raw_ticker"]
        )

        shares = to_float(
            row["shares"]
        )

        price = prices.get(
            ticker
        )

        factor = split_factors.get(
            ticker,
            1.0,
        )

        if (
            shares is not None
            and shares > 0
            and price is not None
        ):
            today_shares = shares * factor

            adjusted_shares.append(
                today_shares
            )

            market_values.append(
                today_shares * price
            )

            continue

        fallback_market_value = to_float(
            row["market_value"]
        )

        row_weight = (
            float(old_weight.iloc[index])
            if index < len(old_weight)
            else 0.0
        )

        missing_weight += max(
            row_weight,
            0.0,
        )

        missing_names.append(
            f"{ticker}({row_weight:.4f}%)"
        )

        if fallback_market_value is None:
            raise HoldingsError(
                f"VGT: unable to value {ticker}"
            )

        adjusted_shares.append(
            shares
        )

        market_values.append(
            fallback_market_value
        )

    if (
        missing_weight
        > MAX_UNPRICED_FALLBACK_WEIGHT["VGT"]
    ):
        raise HoldingsError(
            "VGT: unpriced live tail too material: "
            f"{missing_weight:.4f}% "
            f"({', '.join(missing_names[:10])})"
        )

    total = float(
        sum(market_values)
    )

    if total <= 0:
        raise HoldingsError(
            "VGT: invalid live market-value denominator"
        )

    out["shares"] = adjusted_shares
    out["market_value"] = market_values
    out["weight"] = (
        pd.Series(
            market_values,
            index=out.index,
        )
        / total
        * 100.0
    )

    return out


def fetch_ishares(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:
    response = http_get(
        session,
        ISHARES_ACWI_URL,
        timeout,
        headers={
            **HEADERS,
            "Accept": "text/csv,*/*;q=0.8",
            "Referer": (
                "https://www.ishares.com/us/products/239600/"
                "ishares-msci-acwi-etf"
            ),
        },
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
        and exchange_column
    ):
        frame = frame.copy()

        frame[ticker_column] = [
            qualify_ishares_ticker(
                ticker,
                exchange,
            )
            for ticker, exchange in zip(
                frame[ticker_column],
                frame[exchange_column],
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

    market_value = pd.to_numeric(
        result_all["market_value"],
        errors="coerce",
    )

    provider_weight = pd.to_numeric(
        result_all["weight"],
        errors="coerce",
    ).fillna(0.0)

    missing_market_value = market_value.isna()

    missing_weight = float(
        provider_weight.loc[
            missing_market_value
        ].abs().sum()
    )

    if missing_weight > 0.05:
        raise HoldingsError(
            "ACWI: iShares Market Value is missing "
            f"for rows representing {missing_weight:.4f}% "
            "of reported weight"
        )

    # KEY ACWI FIX:
    # Build the denominator using the complete iShares file BEFORE removing
    # cash/administrative/derivative lines.
    denominator = float(
        market_value.dropna().sum()
    )

    if (
        not math.isfinite(denominator)
        or denominator <= 0
    ):
        raise HoldingsError(
            f"ACWI: invalid iShares denominator {denominator}"
        )

    valid_market_value = market_value.notna()

    result_all.loc[
        valid_market_value,
        "weight",
    ] = (
        market_value.loc[
            valid_market_value
        ]
        / denominator
        * 100.0
    )

    result = drop_nonsecurity_rows(
        result_all
    )

    return FetchResult(
        frame=result,
        source_date=(
            extract_date_from_text(
                text[:30000]
            )
            or iso_today()
        ),
        precision_method=(
            "ishares_market_value_denominator"
        ),
    )


def fetch_ssga(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:
    response = http_get(
        session,
        SSGA_URL.format(
            ticker=etf.lower()
        ),
        timeout,
    )

    frame, source_date = excel_with_detected_header(
        response.content,
        required_tokens=("ticker",),
    )

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

    result = drop_nonsecurity_rows(
        result
    )

    return FetchResult(
        frame=result,
        source_date=(
            source_date
            or iso_today()
        ),
        precision_method="provider_weight",
    )


def _discover_invesco_urls(
    html_text: str,
    base_url: str,
) -> list[str]:
    found: list[str] = []

    for text_tokens, href_tokens in (
        (("export",), ()),
        (("download",), ("holding",)),
        ((), ("holdings",)),
    ):
        url = find_download_link(
            html_text,
            base_url,
            text_contains=text_tokens,
            href_contains=href_tokens,
        )

        if url:
            found.append(
                url
            )

    patterns = [
        (
            r'https?://[^"\'<>\\\s]+'
            r'(?:holding|holdings)'
            r'[^"\'<>\\\s]*'
        ),
        (
            r'(?P<url>/us/(?:en/)?financial-products/'
            r'etfs/holdings/[^"\'<>\\\s]+)'
        ),
    ]

    for pattern in patterns:
        for match in re.finditer(
            pattern,
            html_text,
            flags=re.IGNORECASE,
        ):
            value = (
                match.groupdict().get("url")
                if match.groupdict()
                else match.group(0)
            )

            value = (
                (value or "")
                .replace("\\/", "/")
                .replace("&amp;", "&")
            )

            if value:
                found.append(
                    urljoin(
                        base_url,
                        value,
                    )
                )

    def score(url: str) -> tuple[int, int]:
        lowered = url.lower()
        points = 0

        if "action=download" in lowered:
            points += 5

        if "ticker=" in lowered:
            points += 3

        if "holding" in lowered:
            points += 2

        if lowered.endswith(
            (".csv", ".xls", ".xlsx")
        ):
            points += 2

        return points, -len(url)

    return sorted(
        dict.fromkeys(found),
        key=score,
        reverse=True,
    )


def _parse_invesco_response(
    response: Any,
    *,
    source_date_hint: str,
) -> FetchResult:
    parsed, source_date = parse_download(
        response,
        provider="Invesco",
        source_date_hint=source_date_hint,
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
                "holding name",
                "name",
                "security",
                "description",
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
                "shares held",
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

    result_all, precision = recompute_weights(
        result_all,
        require_all_market_values=False,
    )

    result = drop_nonsecurity_rows(
        result_all
    )

    return FetchResult(
        frame=result,
        source_date=(
            source_date
            or source_date_hint
        ),
        precision_method=precision,
    )


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
                "Accept": HTML_ACCEPT,
                "Upgrade-Insecure-Requests": "1",
            },
            timeout=(10, timeout),
            allow_redirects=True,
        )

        if (
            page.status_code == 200
            and len(page.content) >= 100
        ):
            source_date = (
                extract_date_from_text(
                    page.text[:50000]
                )
                or source_date
            )

            candidates.extend(
                _discover_invesco_urls(
                    page.text,
                    str(page.url),
                )
            )

        else:
            errors.append(
                f"requests page HTTP {page.status_code}, "
                f"{len(page.content)} bytes"
            )

    except Exception as exc:
        errors.append(
            f"requests page: {type(exc).__name__}: {exc}"
        )

    candidates.extend(
        url.format(
            ticker=etf
        )
        for url in INVESCO_EXPORT_URLS
    )

    for url in dict.fromkeys(
        candidates
    ):
        try:
            response = session.get(
                url,
                headers={
                    **HEADERS,
                    "Referer": INVESCO_PAGE_URL,
                    "Accept": DOWNLOAD_ACCEPT,
                },
                timeout=(10, timeout),
                allow_redirects=True,
            )

            if (
                response.status_code != 200
                or len(response.content) < 100
            ):
                errors.append(
                    f"requests export HTTP {response.status_code}, "
                    f"{len(response.content)} bytes"
                )
                continue

            return _parse_invesco_response(
                response,
                source_date_hint=source_date,
            )

        except Exception as exc:
            errors.append(
                f"requests export: {type(exc).__name__}: {exc}"
            )

    print(
        "[SOXQ] Standard Invesco path failed; "
        "retrying with curl_cffi...",
        file=sys.stderr,
    )

    try:
        from curl_cffi import requests as cffi_requests

        browser = cffi_requests.Session(
            impersonate="chrome",
            headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )

        try:
            try:
                browser.get(
                    "https://www.invesco.com/us/",
                    headers={
                        "Accept": HTML_ACCEPT,
                    },
                    timeout=timeout,
                    allow_redirects=True,
                )
            except Exception:
                pass

            browser_source_date = (
                source_date
            )

            browser_candidates: list[str] = []

            try:
                page = browser.get(
                    INVESCO_PAGE_URL,
                    headers={
                        "Accept": HTML_ACCEPT,
                        "Upgrade-Insecure-Requests": "1",
                    },
                    timeout=timeout,
                    allow_redirects=True,
                )

                if (
                    page.status_code == 200
                    and len(page.content) >= 100
                ):
                    browser_source_date = (
                        extract_date_from_text(
                            page.text[:50000]
                        )
                        or browser_source_date
                    )

                    browser_candidates.extend(
                        _discover_invesco_urls(
                            page.text,
                            str(page.url),
                        )
                    )

                else:
                    errors.append(
                        f"curl page HTTP {page.status_code}, "
                        f"{len(page.content)} bytes"
                    )

            except Exception as exc:
                errors.append(
                    f"curl page: {type(exc).__name__}: {exc}"
                )

            browser_candidates.extend(
                url.format(
                    ticker=etf
                )
                for url in INVESCO_EXPORT_URLS
            )

            for url in dict.fromkeys(
                browser_candidates
            ):
                try:
                    response = browser.get(
                        url,
                        headers={
                            "Referer": INVESCO_PAGE_URL,
                            "Accept": DOWNLOAD_ACCEPT,
                        },
                        timeout=timeout,
                        allow_redirects=True,
                    )

                    if (
                        response.status_code != 200
                        or len(response.content) < 100
                    ):
                        errors.append(
                            f"curl export HTTP {response.status_code}, "
                            f"{len(response.content)} bytes"
                        )
                        continue

                    return _parse_invesco_response(
                        response,
                        source_date_hint=browser_source_date,
                    )

                except Exception as exc:
                    errors.append(
                        f"curl export: {type(exc).__name__}: {exc}"
                    )

        finally:
            try:
                browser.close()
            except Exception:
                pass

    except Exception as exc:
        errors.append(
            f"curl_cffi: {type(exc).__name__}: {exc}"
        )

    raise HoldingsError(
        f"{etf}: Invesco unavailable: "
        + " | ".join(errors[-8:])
    )


def fetch_globalx(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:
    page = http_get(
        session,
        GLOBALX_PAGE_URL.format(
            ticker=etf.upper()
        ),
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
            str(page.url),
            text_contains=("full holdings",),
            href_contains=(".csv",),
        )
        or
        find_download_link(
            page.text,
            str(page.url),
            href_contains=(".csv",),
        )
    )

    if not csv_url:
        raise HoldingsError(
            f"{etf}: Global X holdings CSV link not found"
        )

    response = http_get(
        session,
        csv_url,
        timeout,
    )

    parsed, download_date = parse_download(
        response,
        provider="Global X",
        source_date_hint=source_date,
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

    result_all, precision = recompute_weights(
        result_all,
        require_all_market_values=False,
    )

    result = drop_nonsecurity_rows(
        result_all
    )

    return FetchResult(
        frame=result,
        source_date=(
            download_date
            or source_date
        ),
        precision_method=precision,
    )


def parse_firsttrust_dom(
    html_text: str,
) -> pd.DataFrame:
    root = lxml_html.fromstring(
        html_text
    )

    rows: list[dict[str, Any]] = []

    for row in root.xpath("//tr"):
        cells = [
            " ".join(
                clean_text(item)
                for item in cell.itertext()
            ).strip()
            for cell in row.xpath("./th|./td")
        ]

        cells = [
            cell
            for cell in cells
            if cell
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
            or market_value is None
            or shares is None
        ):
            continue

        name = clean_text(
            cells[0]
        )

        ticker = clean_text(
            cells[1]
        )

        if (
            not ticker
            or ticker.lower()
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
                "market_value": market_value,
                "isin": "",
                "sedol": "",
            }
        )

    if not rows:
        raise HoldingsError(
            "GRID: no First Trust holdings rows found"
        )

    return pd.DataFrame(
        rows,
        columns=STD_COLS,
    )


def fetch_firsttrust(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:
    page = http_get(
        session,
        FIRSTTRUST_URL.format(
            ticker=etf
        ),
        timeout,
    )

    source_date = (
        extract_date_from_text(
            page.text[:50000]
        )
        or iso_today()
    )

    result_all = parse_firsttrust_dom(
        page.text
    )

    result_all, precision = recompute_weights(
        result_all,
        require_all_market_values=True,
    )

    result = drop_nonsecurity_rows(
        result_all
    )

    return FetchResult(
        frame=result,
        source_date=source_date,
        precision_method=precision,
    )


def _parse_vaneck_download(
    response: Any,
    *,
    source_date_hint: str,
) -> FetchResult:
    parsed, source_date = parse_download(
        response,
        provider="VanEck",
        source_date_hint=source_date_hint,
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

    result_all, precision = recompute_weights(
        result_all,
        require_all_market_values=False,
    )

    result = drop_nonsecurity_rows(
        result_all
    )

    return FetchResult(
        frame=result,
        source_date=(
            source_date
            or source_date_hint
        ),
        precision_method=precision,
    )


def fetch_vaneck(
    session: requests.Session,
    etf: str,
    timeout: int,
) -> FetchResult:
    errors: list[str] = []
    source_date = iso_today()

    try:
        response = session.get(
            VANECK_XLSX_URL,
            headers={
                **HEADERS,
                "Referer": VANECK_PAGE_URL,
                "Accept": DOWNLOAD_ACCEPT,
            },
            timeout=(10, timeout),
            allow_redirects=True,
        )

        if (
            response.status_code == 200
            and len(response.content) >= 100
        ):
            return _parse_vaneck_download(
                response,
                source_date_hint=source_date,
            )

        errors.append(
            f"direct XLSX HTTP {response.status_code}, "
            f"{len(response.content)} bytes"
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
                str(page.url),
                text_contains=("download",),
                href_contains=("holding",),
            )
            or
            find_download_link(
                page.text,
                str(page.url),
                href_contains=(".xls",),
            )
        )

        if not xls_url:
            raise HoldingsError(
                "holdings XLS link not found"
            )

        response = http_get(
            session,
            xls_url,
            timeout,
        )

        return _parse_vaneck_download(
            response,
            source_date_hint=source_date,
        )

    except Exception as exc:
        errors.append(
            f"page discovery: {exc}"
        )

    raise HoldingsError(
        f"{etf}: "
        + " | ".join(errors)
    )


def canonical_ticker(
    raw: str,
) -> str:
    ticker = clean_text(
        raw
    ).upper()

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
        .upper()
    )

    text = re.sub(
        r"[^A-Z0-9& ]+",
        " ",
        text,
    )

    return re.sub(
        r"\s+",
        " ",
        text,
    ).strip()


def identity_key(
    row: pd.Series,
) -> str:
    isin = clean_text(
        row.get(
            "isin",
            "",
        )
    ).upper()

    sedol = clean_text(
        row.get(
            "sedol",
            "",
        )
    ).upper()

    ticker = canonical_ticker(
        row.get(
            "raw_ticker",
            "",
        )
    )

    name = normalized_name(
        row.get(
            "raw_name",
            "",
        )
    )

    if re.fullmatch(
        r"[A-Z]{2}[A-Z0-9]{9}[0-9]",
        isin,
    ):
        return (
            "ISIN:"
            + isin
        )

    if re.fullmatch(
        r"[A-Z0-9]{7}",
        sedol,
    ):
        return (
            "SEDOL:"
            + sedol
        )

    return (
        f"TK:{ticker}|{name}"
    )


def canonicalize_across_etfs(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    out = frame.copy()

    out["_identity"] = out.apply(
        identity_key,
        axis=1,
    )

    out["_ticker"] = out[
        "raw_ticker"
    ].map(
        canonical_ticker
    )

    out["_name"] = out[
        "raw_name"
    ].map(
        clean_text
    )

    ticker_map: dict[str, str] = {}
    name_map: dict[str, str] = {}

    for identity, group in out.groupby(
        "_identity",
        sort=False,
    ):
        tickers = [
            value
            for value in group["_ticker"]
            if value
        ]

        names = [
            value
            for value in group["_name"]
            if value
        ]

        if tickers:
            ticker_counts = pd.Series(
                tickers
            ).value_counts()

            ticker_map[
                str(identity)
            ] = str(
                ticker_counts.index[0]
            )

        else:
            ticker_map[
                str(identity)
            ] = ""

        if names:
            name_counts = pd.Series(
                names
            ).value_counts()

            top_count = int(
                name_counts.iloc[0]
            )

            tied = [
                str(name)
                for name, count in name_counts.items()
                if int(count) == top_count
            ]

            tied.sort(
                key=lambda value: (
                    value.isupper(),
                    len(value),
                    value.upper(),
                )
            )

            name_map[
                str(identity)
            ] = tied[0]

        else:
            name_map[
                str(identity)
            ] = ticker_map[
                str(identity)
            ]

    out["canonical_ticker"] = out[
        "_identity"
    ].map(
        lambda value: ticker_map[
            str(value)
        ]
    )

    out["canonical_name"] = out[
        "_identity"
    ].map(
        lambda value: name_map[
            str(value)
        ]
    )

    return out


def validate_live_frame(
    etf: str,
    frame: pd.DataFrame,
    previous_count: int | None,
) -> None:
    if frame.empty:
        raise HoldingsError(
            f"{etf}: no holdings"
        )

    required = MIN_HOLDINGS[
        etf
    ]

    if (
        previous_count
        and previous_count >= required
    ):
        required = max(
            required,
            math.floor(
                previous_count
                * MIN_PREVIOUS_RATIO
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
        weights.notna().sum()
    )

    if (
        numeric_count
        < math.floor(
            len(frame)
            * 0.97
        )
    ):
        raise HoldingsError(
            f"{etf}: only {numeric_count}/{len(frame)} "
            "rows have numeric weights"
        )

    if weights.dropna().lt(
        -0.05
    ).any():
        raise HoldingsError(
            f"{etf}: materially negative security weight"
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
            f"{etf}: weight sum {total:.6f}% outside "
            f"[{MIN_TOTAL_WEIGHT}, {MAX_TOTAL_WEIGHT}]"
        )


def load_previous(
    path: Path,
) -> dict[str, pd.DataFrame]:
    if not path.exists():
        return {}

    try:
        previous = pd.read_csv(
            path
        )
    except Exception as exc:
        print(
            f"WARNING: could not read previous CSV: {exc}",
            file=sys.stderr,
        )
        return {}

    if list(
        previous.columns
    ) != OUTPUT_COLS:
        print(
            "WARNING: previous CSV schema mismatch; "
            "ignoring previous file",
            file=sys.stderr,
        )
        return {}

    result: dict[str, pd.DataFrame] = {}

    for ticker, group in previous.groupby(
        "fund_ticker",
        sort=False,
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


def sanitize_previous(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    out = frame[
        OUTPUT_COLS
    ].copy()

    out["holding_ticker"] = out[
        "holding_ticker"
    ].map(
        clean_text
    )

    out["holding_name"] = out[
        "holding_name"
    ].map(
        clean_text
    )

    keep = [
        not is_nonsecurity(
            ticker,
            name,
        )
        for ticker, name in zip(
            out["holding_ticker"],
            out["holding_name"],
        )
    ]

    out = (
        out
        .loc[keep]
        .reset_index(
            drop=True
        )
    )

    out["rank"] = range(
        1,
        len(out) + 1,
    )

    return out


def validate_output(
    etf: str,
    frame: pd.DataFrame,
) -> None:
    if frame.empty:
        raise HoldingsError(
            f"{etf}: output rows empty"
        )

    if len(frame) < MIN_HOLDINGS[
        etf
    ]:
        raise HoldingsError(
            f"{etf}: output has only {len(frame)} rows"
        )

    weights = pd.to_numeric(
        frame["weight"],
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
            f"{etf}: output weight sum {total:.6f}% invalid"
        )


def source_age_days(
    frame: pd.DataFrame,
) -> int:
    dates = pd.to_datetime(
        frame["source_date"],
        errors="coerce",
    ).dropna()

    if dates.empty:
        return 9999

    return (
        utc_now().date()
        - dates.max().date()
    ).days


def previous_retrieval_date(
    frame: pd.DataFrame,
    default_date: date,
) -> date:
    parsed = pd.to_datetime(
        frame["retrieved_at_utc"],
        errors="coerce",
        utc=True,
    ).dropna()

    if parsed.empty:
        return default_date

    return (
        parsed.max().date()
    )


def drift_previous_snapshot(
    frame: pd.DataFrame,
    *,
    fund: str,
    retrieved_at: datetime,
) -> pd.DataFrame:
    if fund not in {
        "VGT",
        "SOXQ",
    }:
        raise HoldingsError(
            f"{fund}: drift fallback not supported"
        )

    out = frame.copy()

    age = source_age_days(
        out
    )

    if fund == "VGT":
        warn_days = VGT_WARN_DRIFT_DAYS
        max_days = VGT_MAX_DRIFT_DAYS
    else:
        warn_days = SOXQ_WARN_DRIFT_DAYS
        max_days = SOXQ_MAX_DRIFT_DAYS

    if age > max_days:
        raise HoldingsError(
            f"{fund}: official composition is {age} days old; "
            f"refusing to drift past {max_days} days"
        )

    if age > warn_days:
        print(
            f"[{fund}] WARNING: official composition "
            f"snapshot is {age} days old",
            file=sys.stderr,
        )

    shares = pd.to_numeric(
        out["shares_held"],
        errors="coerce",
    )

    old_weights = pd.to_numeric(
        out["weight"],
        errors="coerce",
    ).fillna(0.0)

    eligible = (
        shares.notna()
        & (shares > 0)
        & out["holding_ticker"]
        .astype(str)
        .map(clean_text)
        .ne("")
    )

    if int(
        eligible.sum()
    ) < MIN_HOLDINGS[
        fund
    ]:
        raise HoldingsError(
            f"{fund}: only {int(eligible.sum())} rows "
            "have usable share counts"
        )

    source_dates = pd.to_datetime(
        out["source_date"],
        errors="coerce",
    ).dropna()

    official_date = (
        source_dates.max().date()
        if not source_dates.empty
        else utc_now().date()
    )

    since_date = previous_retrieval_date(
        out,
        official_date,
    )

    prices, split_factors = yahoo_prices_and_splits(
        out.loc[
            eligible,
            "holding_ticker",
        ].map(clean_text),
        since_date,
    )

    missing_indices: list[int] = []

    for index in out.index[
        eligible
    ]:
        ticker = clean_text(
            out.at[
                index,
                "holding_ticker",
            ]
        )

        share_count = to_float(
            out.at[
                index,
                "shares_held",
            ]
        )

        price = prices.get(
            ticker
        )

        factor = split_factors.get(
            ticker,
            1.0,
        )

        if (
            share_count is None
            or share_count <= 0
            or price is None
        ):
            missing_indices.append(
                index
            )
            continue

        adjusted_shares = (
            share_count
            * factor
        )

        out.at[
            index,
            "shares_held",
        ] = adjusted_shares

        out.at[
            index,
            "market_value_usd",
        ] = (
            adjusted_shares
            * price
        )

    if missing_indices:
        missing_weight = float(
            old_weights.loc[
                missing_indices
            ]
            .clip(lower=0)
            .sum()
        )

        threshold = (
            MAX_UNPRICED_FALLBACK_WEIGHT[
                fund
            ]
        )

        if missing_weight > threshold:
            names = ", ".join(
                clean_text(
                    out.at[
                        index,
                        "holding_ticker",
                    ]
                )
                for index in missing_indices[:10]
            )

            raise HoldingsError(
                f"{fund}: Yahoo could not reprice "
                f"{len(missing_indices)} holdings representing "
                f"{missing_weight:.4f}% old weight "
                f"(limit {threshold:.4f}%): {names}"
            )

        print(
            f"[{fund}] Tiny unpriced tail retained "
            f"at old market value: {missing_weight:.4f}%",
            file=sys.stderr,
        )

    market_value = pd.to_numeric(
        out["market_value_usd"],
        errors="coerce",
    )

    if not market_value.notna().all():
        raise HoldingsError(
            f"{fund}: {int(market_value.isna().sum())} "
            "market values remain blank"
        )

    total = float(
        market_value.sum()
    )

    if (
        not math.isfinite(total)
        or total <= 0
    ):
        raise HoldingsError(
            f"{fund}: invalid drift denominator {total}"
        )

    # KEY SOXQ/VGT FALLBACK FIX:
    # Recompute weights using ALL current repriced market values.
    out["weight"] = (
        market_value
        / total
        * 100.0
    )

    out["retrieved_at_utc"] = iso_utc(
        retrieved_at
    )

    out = (
        out
        .sort_values(
            "weight",
            ascending=False,
            na_position="last",
        )
        .reset_index(
            drop=True
        )
    )

    out["rank"] = range(
        1,
        len(out) + 1,
    )

    return out[
        OUTPUT_COLS
    ]


def generic_market_value_backfill(
    frame: pd.DataFrame,
    *,
    retrieved_at: datetime,
) -> pd.DataFrame:
    out = frame.copy()

    market_value = pd.to_numeric(
        out["market_value_usd"],
        errors="coerce",
    )

    shares = pd.to_numeric(
        out["shares_held"],
        errors="coerce",
    )

    need = (
        market_value.isna()
        & shares.notna()
        & (shares > 0)
        & out["holding_ticker"]
        .astype(str)
        .map(clean_text)
        .ne("")
    )

    if not need.any():
        return out

    prices = yahoo_latest_prices(
        out.loc[
            need,
            "holding_ticker",
        ].map(clean_text)
    )

    filled = 0

    for index in out.index[
        need
    ]:
        ticker = clean_text(
            out.at[
                index,
                "holding_ticker",
            ]
        )

        share_count = to_float(
            out.at[
                index,
                "shares_held",
            ]
        )

        price = prices.get(
            ticker
        )

        if (
            price is None
            or share_count is None
        ):
            continue

        out.at[
            index,
            "market_value_usd",
        ] = (
            share_count
            * price
        )

        filled += 1

    if filled:
        out["retrieved_at_utc"] = iso_utc(
            retrieved_at
        )

    return out


def normalized_output(
    etf: str,
    canonical_frame: pd.DataFrame,
    source_date: str,
    retrieved_at: datetime,
) -> pd.DataFrame:
    fund = canonical_frame[
        canonical_frame[
            "etf"
        ].eq(etf)
    ].copy()

    fund["weight"] = pd.to_numeric(
        fund["weight"],
        errors="coerce",
    )

    fund["shares"] = pd.to_numeric(
        fund["shares"],
        errors="coerce",
    )

    fund["market_value"] = pd.to_numeric(
        fund["market_value"],
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

    out = pd.DataFrame(
        {
            "source_date": (
                source_date
            ),
            "retrieved_at_utc": iso_utc(
                retrieved_at
            ),
            "fund_ticker": etf,
            "provider": PROVIDER_LABELS[
                etf
            ],
            "rank": range(
                1,
                len(fund) + 1,
            ),
            "holding_ticker": fund[
                "canonical_ticker"
            ],
            "holding_name": fund[
                "canonical_name"
            ],
            "weight": fund[
                "weight"
            ],
            "shares_held": fund[
                "shares"
            ],
            "market_value_usd": fund[
                "market_value"
            ],
        }
    )

    return out[
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

    temporary = destination.with_name(
        f".{destination.name}.tmp"
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
            or temporary.stat().st_size == 0
        ):
            raise HoldingsError(
                f"Generated empty CSV: {temporary}"
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
        print(
            f"[{etf}] Fetching {PROVIDER_LABELS[etf]}...",
            file=sys.stderr,
        )

        try:
            result = FETCHERS[
                etf
            ](
                session,
                etf,
                timeout,
            )

            frame = result.frame.copy()

            if etf == "VGT":
                frame = drift_vgt_live_snapshot(
                    frame,
                    result.source_date,
                )

                result.precision_method = (
                    "vanguard_shares_x_live_price"
                )

            previous_count = (
                len(previous[etf])
                if etf in previous
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
            ] = result.source_date

            precision_methods[
                etf
            ] = result.precision_method

            print(
                f"[{etf}] OK: "
                f"{len(frame)} rows, "
                f"{pd.to_numeric(frame['weight'], errors='coerce').sum():.8f}% "
                f"({result.precision_method})",
                file=sys.stderr,
            )

        except Exception as exc:
            failures[
                etf
            ] = str(exc)

            print(
                f"[{etf}] LIVE FAILED: {exc}",
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

    retrieved_at = utc_now()

    canonical_live: pd.DataFrame | None = None

    if live:
        stacked = pd.concat(
            [
                live[etf]
                for etf in etfs
                if etf in live
            ],
            ignore_index=True,
        )

        canonical_live = canonicalize_across_etfs(
            stacked
        )

    final_by_etf: dict[str, pd.DataFrame] = {}
    source_status: dict[str, str] = {}
    fatal: dict[str, str] = {}

    for etf in etfs:
        if (
            etf in live
            and canonical_live is not None
        ):
            output = normalized_output(
                etf,
                canonical_live,
                source_dates[etf],
                retrieved_at,
            )

            validate_output(
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

        if etf not in previous:
            fatal[
                etf
            ] = (
                f"live failed "
                f"({live_failures.get(etf, 'unknown')}); "
                "no last-known-good rows"
            )

            continue

        try:
            fallback = sanitize_previous(
                previous[
                    etf
                ].copy()
            )

            validate_output(
                etf,
                fallback,
            )

            if etf in {
                "VGT",
                "SOXQ",
            }:
                fallback = drift_previous_snapshot(
                    fallback,
                    fund=etf,
                    retrieved_at=retrieved_at,
                )

                fallback_mode = (
                    "last official shares x live Yahoo prices"
                )

            else:
                fallback = generic_market_value_backfill(
                    fallback,
                    retrieved_at=retrieved_at,
                )

                fallback_mode = (
                    "last-known-good"
                )

            validate_output(
                etf,
                fallback,
            )

            final_by_etf[
                etf
            ] = fallback[
                OUTPUT_COLS
            ]

            source_status[
                etf
            ] = fallback_mode

            print(
                f"[{etf}] Using {fallback_mode}; "
                "source_date preserved",
                file=sys.stderr,
            )

        except Exception as exc:
            fatal[
                etf
            ] = (
                f"live failed "
                f"({live_failures.get(etf, 'unknown')}); "
                f"fallback failed ({exc})"
            )

    if fatal:
        print(
            "\nUpdate aborted. Existing combined CSV "
            "was NOT replaced.",
            file=sys.stderr,
        )

        for etf, reason in fatal.items():
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
            for etf in etfs
        ],
        ignore_index=True,
    )

    if list(
        combined.columns
    ) != OUTPUT_COLS:
        print(
            f"ERROR: output schema changed: {list(combined.columns)}",
            file=sys.stderr,
        )

        return 1

    expected_funds = set(
        etfs
    )

    actual_funds = set(
        combined[
            "fund_ticker"
        ]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
    )

    if (
        actual_funds
        != expected_funds
    ):
        print(
            "ERROR: ETF set mismatch. "
            f"Expected={expected_funds}, Actual={actual_funds}",
            file=sys.stderr,
        )

        return 1

    for etf, group in combined.groupby(
        "fund_ticker",
        sort=False,
    ):
        validate_output(
            str(etf),
            group,
        )

    write_csv_atomic(
        combined,
        combined_path,
    )

    print(
        f"\nPublished {len(combined):,} rows "
        f"across {len(etfs)} ETFs -> {combined_path}",
        file=sys.stderr,
    )

    print(
        "Sources used:",
        file=sys.stderr,
    )

    for etf in etfs:
        print(
            f"  - {etf}: {source_status[etf]}",
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
            "Optional ETF tickers. Defaults to the "
            "9-fund production basket."
        ),
    )

    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
    )

    parser.add_argument(
        "--combined-name",
        default=DEFAULT_COMBINED_NAME,
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
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
            "ERROR: --combined-name must be a filename, not a path",
            file=sys.stderr,
        )

        return 2

    if args.timeout <= 0:
        print(
            "ERROR: --timeout must be > 0",
            file=sys.stderr,
        )

        return 2

    etfs = (
        [
            ticker.strip().upper()
            for ticker in args.tickers
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
        for etf in etfs
        if etf not in FETCHERS
    ]

    if unknown:
        print(
            f"ERROR: unknown ETF(s): {unknown}. "
            f"Known={list(FETCHERS)}",
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
