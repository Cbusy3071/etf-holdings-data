"""
Standalone ETF holdings updater.

Usage:
    python update_holdings.py \
        --out-dir data \
        --combined-name ETF_Holdings_Latest.csv \
        VGT ACWI XLF XLI XLC PPH MLPX GRID SOXQ

Dependencies:
    pip install pandas requests yfinance openpyxl lxml html5lib curl_cffi selenium

Important VGT behaviour:
- VGT uses Vanguard's provider-reported holdings weights.
- VGT weights are NEVER synthetically drifted with Yahoo prices.
- Vanguard source_date must be a genuine month-end holdings date.
- If the normal Vanguard API is blocked, the script retries through curl_cffi,
  then through Chrome/Selenium in a same-origin browser session.
- If Vanguard is unavailable, only a validated prior OFFICIAL month-end VGT
  snapshot may be retained unchanged. A non-month-end prior VGT snapshot is
  rejected because it may be a synthetic/stale snapshot from an older script.

Output schema is kept stable for Power Query.
"""

from __future__ import annotations

import argparse
import html as html_lib
import io
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
    "SOXQ": "Schwab/WSOD",
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
SOXQ_WARN_DRIFT_DAYS = 2
SOXQ_MAX_DRIFT_DAYS = 5
MAX_UNPRICED_SOXQ_FALLBACK_WEIGHT = 0.10

SSGA_URL = (
    "https://www.ssga.com/us/en/intermediary/etfs/"
    "library-content/products/fund-data/etfs/us/"
    "holdings-daily-us-en-{ticker}.xlsx"
)

ISHARES_ACWI_URL = (
    "https://www.ishares.com/us/products/239600/"
    "ishares-msci-acwi-etf/latest-holdings.csv"
)

VANGUARD_API_URL = (
    "https://investor.vanguard.com/investment-products/etfs/profile/api/"
    "{ticker}/portfolio-holding/stock"
)
VANGUARD_PROFILE_URL = (
    "https://investor.vanguard.com/investment-products/etfs/profile/{ticker}"
)

SCHWAB_SOXQ_URL = (
    "https://www.schwab.wallst.com/schwab/Prospect/research/etfs/"
    "schwabETF/index.asp?type=holdings&symbol={ticker}"
)
GLOBALX_PAGE_URL = "https://www.globalxetfs.com/funds/{ticker}"
FIRSTTRUST_URL = (
    "https://www.ftportfolios.com/Retail/Etf/EtfHoldings.aspx?Ticker={ticker}"
)
VANECK_PAGE_URL = (
    "https://www.vaneck.com/us/en/investments/pharmaceutical-etf-pph/holdings/"
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
    title_match = re.search(r'''title\s*=\s*["']([^"']+)["']''', text, re.I)
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
    text = text.replace(",", "").replace("%", "").replace("$", "").strip("() ")
    text = re.sub(r"[^0-9eE+\-.]", "", text)
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return -number if negative else number


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
        r"\b(?:20\d{2}-\d{1,2}-\d{1,2}|\d{1,2}/\d{1,2}/20\d{2}|"
        r"[A-Za-z]+\s+\d{1,2},?\s+20\d{2})\b",
        text[:50000],
    )
    for candidate in candidates:
        parsed = normalize_date_value(candidate)
        if parsed:
            return parsed
    return None


def is_month_end(date_text: str) -> bool:
    try:
        d = datetime.fromisoformat(date_text).date()
    except Exception:
        return False
    return (d + timedelta(days=1)).month != d.month


def validate_vanguard_source_date(source_date: str) -> None:
    """Vanguard's public full-holdings series is month-end. Reject fake dates."""
    if not is_month_end(source_date):
        raise HoldingsError(
            f"VGT: Vanguard source_date {source_date!r} is not month-end; "
            "refusing to label it as an official holdings snapshot"
        )
    parsed = datetime.fromisoformat(source_date).date()
    if parsed > utc_now().date():
        raise HoldingsError(f"VGT: future Vanguard source_date {source_date}")
    if (utc_now().date() - parsed).days > 70:
        raise HoldingsError(
            f"VGT: Vanguard holdings snapshot {source_date} is implausibly old"
        )


def find_vanguard_holdings_date(payload: Any) -> str | None:
    """
    Find only explicitly holdings/portfolio/as-of style dates.

    Deliberately DO NOT accept a generic key named 'date'. The old script did,
    which could turn an unrelated 2026-08-26 date into VGT's source_date even
    though the actual holdings snapshot was 2026-07-31.
    """
    strong_keys = {
        "asofdate",
        "as_of_date",
        "asof",
        "holdingsdate",
        "holdings_date",
        "portfoliodate",
        "portfolio_date",
        "effectivedate",
        "effective_date",
    }
    candidates: list[str] = []
    stack = [payload]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for key, value in item.items():
                if str(key).lower().replace("-", "_") in strong_keys:
                    parsed = normalize_date_value(value)
                    if parsed:
                        candidates.append(parsed)
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)

    # Prefer a genuine month-end. If several are present, use the latest.
    month_ends = sorted({d for d in candidates if is_month_end(d)})
    return month_ends[-1] if month_ends else None


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    columns = [(str(c), str(c).strip().lower()) for c in df.columns]
    for candidate in candidates:
        needle = candidate.lower()
        for original, lowered in columns:
            if needle in lowered:
                return original
    return None


def _series(df: pd.DataFrame, col: str | None, mapper: Callable[[Any], Any], default: Any) -> pd.Series:
    if col and col in df.columns:
        return df[col].map(mapper)
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
    out = out[out["raw_name"].ne("") | out["raw_ticker"].ne("")].copy()
    out["isin"] = out["isin"].astype(str).str.strip().str.upper()
    out["sedol"] = out["sedol"].astype(str).str.strip().str.upper()
    return out.reset_index(drop=True)[STD_COLS]


def is_nonsecurity(ticker: str, name: str) -> bool:
    t = clean_text(ticker).upper().strip()
    n = clean_text(name).upper().strip()
    if not t:
        return True
    if t in {
        "TICKER", "SYMBOL", "N/A", "NA", "NONE", "--", "-", "USD", "EUR",
        "GBP", "CHF", "CAD", "AUD", "JPY", "HKD", "CNY", "TWD", "KRW",
        "AGPXX", "BNYMLEND",
    }:
        return True
    if t.startswith("$") or "CASH" in t:
        return True
    if any(token in n for token in (
        "US DOLLAR", "USD CASH", "OTHER/CASH", "SECURITIES LENDING",
        "GOVERNMENT & AGENCY PORTFOLIO",
    )):
        return True
    if any(token in n for token in ("FUTURE", "FUTURES", "FX FORWARD", "FORWARD CONTRACT")):
        return True
    return False


def drop_nonsecurity_rows(df: pd.DataFrame) -> pd.DataFrame:
    keep = [not is_nonsecurity(t, n) for t, n in zip(df["raw_ticker"], df["raw_name"])]
    return df.loc[keep].reset_index(drop=True)


def http_get(
    session: requests.Session,
    url: str,
    timeout: int,
    *,
    headers: dict[str, str] | None = None,
) -> requests.Response:
    response = session.get(url, headers=headers, timeout=(10, timeout), allow_redirects=True)
    if response.status_code != 200:
        raise HoldingsError(f"HTTP {response.status_code}: {url}")
    if len(response.content) < 100:
        raise HoldingsError(f"Suspiciously small response ({len(response.content)} bytes): {url}")
    return response


def sniff_csv(text: str, required: list[str]) -> pd.DataFrame:
    lines = text.splitlines()
    header_index = None
    for i, line in enumerate(lines[:80]):
        lowered = line.lower()
        if all(token.lower() in lowered for token in required):
            header_index = i
            break
    if header_index is None:
        raise HoldingsError(f"CSV header containing {required} not found")
    frame = pd.read_csv(io.StringIO("\n".join(lines[header_index:])), dtype=str, low_memory=False)
    frame.columns = [clean_text(c) for c in frame.columns]
    return frame


def excel_with_detected_header(
    content: bytes,
    *,
    required_tokens: tuple[str, ...] = ("ticker",),
) -> tuple[pd.DataFrame, str | None]:
    raw = pd.read_excel(io.BytesIO(content), header=None, dtype=str)
    header_index = None
    for i in range(min(50, len(raw))):
        row_text = " | ".join(clean_text(v).lower() for v in raw.iloc[i].tolist())
        if all(token.lower() in row_text for token in required_tokens):
            header_index = i
            break
    if header_index is None:
        raise HoldingsError(f"Excel header containing {required_tokens} not found")
    source_date = None
    for i in range(header_index):
        parsed = extract_date_from_text(" ".join(clean_text(v) for v in raw.iloc[i].tolist()))
        if parsed:
            source_date = parsed
            break
    frame = raw.iloc[header_index + 1 :].copy()
    frame.columns = [clean_text(c) for c in raw.iloc[header_index].tolist()]
    return frame, source_date


def parse_download(response: Any, *, provider: str, source_date_hint: str | None = None) -> tuple[pd.DataFrame, str | None]:
    content_type = str(response.headers.get("content-type", "")).lower()
    final_url = str(response.url).lower()
    content = response.content
    if (
        "spreadsheet" in content_type
        or "excel" in content_type
        or final_url.endswith((".xls", ".xlsx"))
        or content[:2] == b"PK"
    ):
        frame, source_date = excel_with_detected_header(content, required_tokens=("ticker",))
        return frame, source_date or source_date_hint
    text = response.text
    if "ticker" in text[:15000].lower() and "," in text[:3000]:
        return sniff_csv(text, ["ticker"]), extract_date_from_text(text[:30000]) or source_date_hint
    raise HoldingsError(
        f"{provider}: download not recognizable CSV/Excel "
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
        if not href or href.lower().startswith(("javascript:", "mailto:", "#")):
            continue
        label = " ".join(clean_text(item) for item in anchor.itertext()).lower()
        href_lower = href.lower()
        if text_contains and not all(token.lower() in label for token in text_contains):
            continue
        if href_contains and not all(token.lower() in href_lower for token in href_contains):
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
        ranked.append((score, urljoin(base_url, href)))
    if not ranked:
        return None
    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1]


def recompute_weights(frame: pd.DataFrame, *, require_all_market_values: bool) -> tuple[pd.DataFrame, str]:
    out = frame.copy()
    market_value = pd.to_numeric(out["market_value"], errors="coerce")
    if require_all_market_values and not market_value.notna().all():
        return out, "provider_weight"
    valid = market_value.notna()
    if not valid.any():
        return out, "provider_weight"
    total = float(market_value.loc[valid].sum())
    if not math.isfinite(total) or total <= 0:
        return out, "provider_weight"
    out.loc[valid, "weight"] = market_value.loc[valid] / total * 100.0
    return out, "market_value_recomputed"


def qualify_ishares_ticker(raw_ticker: Any, exchange: Any) -> str:
    ticker = clean_text(raw_ticker)
    exchange_name = clean_text(exchange)
    if not ticker:
        return ""
    suffix = ISHARES_EXCHANGE_SUFFIX.get(exchange_name)
    if suffix and "." not in ticker:
        return ticker + suffix
    return ticker


# ----------------------------- Vanguard / VGT -----------------------------

def _walk_dict_lists(value: Any):
    if isinstance(value, list):
        if value and all(isinstance(item, dict) for item in value):
            yield value
        for item in value:
            yield from _walk_dict_lists(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _walk_dict_lists(item)


def _score_holdings_list(rows: list[dict[str, Any]]) -> int:
    score = 0
    for row in rows[:20]:
        keys = {str(k).lower() for k in row}
        if {"ticker", "tickersymbol", "symbol"} & keys:
            score += 3
        if {"sharesheld", "shares", "quantity"} & keys:
            score += 3
        if {"marketvalue", "marketval"} & keys:
            score += 2
        if {"percentweight", "weight", "percent"} & keys:
            score += 3
        if {"longname", "shortname", "name", "securityname"} & keys:
            score += 1
    return score


def parse_vanguard_payload(payload: Any, etf: str, source_date_hint: str | None = None) -> FetchResult:
    candidates = list(_walk_dict_lists(payload))
    if not candidates:
        raise HoldingsError(f"{etf}: no holdings list in Vanguard JSON")
    entities = max(candidates, key=_score_holdings_list)
    if _score_holdings_list(entities) <= 0:
        raise HoldingsError(f"{etf}: Vanguard JSON had no recognizable holdings")

    rows: list[dict[str, Any]] = []
    for entity in entities:
        rows.append({
            "raw_ticker": clean_text(entity.get("ticker") or entity.get("tickerSymbol") or entity.get("symbol") or ""),
            "raw_name": clean_text(
                entity.get("longName") or entity.get("shortName") or entity.get("name")
                or entity.get("securityName") or ""
            ),
            "weight": to_float(entity.get("percentWeight") or entity.get("weight") or entity.get("percent")),
            "shares": to_float(entity.get("sharesHeld") or entity.get("shares") or entity.get("quantity")),
            "market_value": to_float(entity.get("marketValue") or entity.get("marketVal") or entity.get("value")),
            "isin": clean_text(entity.get("isin") or ""),
            "sedol": clean_text(entity.get("sedol") or ""),
        })

    frame = drop_nonsecurity_rows(pd.DataFrame(rows, columns=STD_COLS))
    source_date = find_vanguard_holdings_date(payload) or source_date_hint
    if not source_date:
        raise HoldingsError(
            f"{etf}: Vanguard holdings loaded but no trustworthy month-end holdings date was found"
        )
    validate_vanguard_source_date(source_date)

    # CRITICAL FIX: keep Vanguard's own weights. Do NOT convert shares to current
    # Yahoo market values and renormalise them.
    return FetchResult(
        frame=frame,
        source_date=source_date,
        precision_method="vanguard_provider_weight",
    )


def _make_chrome_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service

    options = webdriver.ChromeOptions()
    for arg in (
        "--headless=new",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--window-size=1920,1080",
        "--lang=en-US",
    ):
        options.add_argument(arg)
    options.add_argument(f"--user-agent={HEADERS['User-Agent']}")
    chrome_binary = os.environ.get("CHROME_BIN")
    if chrome_binary:
        options.binary_location = chrome_binary
    chromedriver_binary = os.environ.get("CHROMEDRIVER")
    service = Service(executable_path=chromedriver_binary) if chromedriver_binary else Service()
    return webdriver.Chrome(service=service, options=options)


def _vanguard_page_month_end(body_text: str) -> str | None:
    # Prefer text near holdings language rather than a random date elsewhere.
    patterns = [
        r"(?is)holding\s+details.{0,400}?as\s+of\s+([A-Za-z]+\s+\d{1,2},?\s+20\d{2}|\d{1,2}/\d{1,2}/20\d{2})",
        r"(?is)portfolio\s+composition\s+file.{0,400}?as\s+of\s+([A-Za-z]+\s+\d{1,2},?\s+20\d{2}|\d{1,2}/\d{1,2}/20\d{2})",
        r"(?i)as\s+of\s+(\d{1,2}/\d{1,2}/20\d{2})",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, body_text):
            parsed = normalize_date_value(match.group(1))
            if parsed and is_month_end(parsed):
                return parsed
    return None


def fetch_vanguard(session: requests.Session, etf: str, timeout: int) -> FetchResult:
    api_url = VANGUARD_API_URL.format(ticker=etf.lower())
    profile_url = VANGUARD_PROFILE_URL.format(ticker=etf.lower())
    errors: list[str] = []

    # 1) Normal requests.
    try:
        response = session.get(
            api_url,
            headers={**HEADERS, "Accept": "application/json,text/plain,*/*", "Referer": profile_url},
            timeout=(10, timeout),
            allow_redirects=True,
        )
        if response.status_code == 200 and len(response.content) >= 100:
            try:
                return parse_vanguard_payload(response.json(), etf)
            except Exception as exc:
                errors.append(f"requests parse: {exc}")
        else:
            errors.append(f"requests HTTP {response.status_code}, {len(response.content)} bytes")
    except Exception as exc:
        errors.append(f"requests: {type(exc).__name__}: {exc}")

    # 2) Browser impersonation without Selenium.
    try:
        from curl_cffi import requests as cffi_requests
        browser = cffi_requests.Session(
            impersonate="chrome",
            headers={"Accept-Language": "en-US,en;q=0.9", "Cache-Control": "no-cache"},
        )
        try:
            # Prime Vanguard cookies first.
            browser.get(profile_url, timeout=timeout, allow_redirects=True)
            response = browser.get(
                api_url,
                headers={"Accept": "application/json,text/plain,*/*", "Referer": profile_url},
                timeout=timeout,
                allow_redirects=True,
            )
            if response.status_code == 200 and len(response.content) >= 100:
                try:
                    return parse_vanguard_payload(response.json(), etf)
                except Exception as exc:
                    errors.append(f"curl_cffi parse: {exc}")
            else:
                errors.append(f"curl_cffi HTTP {response.status_code}, {len(response.content)} bytes")
        finally:
            try:
                browser.close()
            except Exception:
                pass
    except Exception as exc:
        errors.append(f"curl_cffi: {type(exc).__name__}: {exc}")

    # 3) Full browser fallback. Open the fund page so Vanguard JS/cookies run,
    # then fetch the API from the same origin in that browser session.
    driver = None
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait

        driver = _make_chrome_driver()
        driver.set_page_load_timeout(max(30, timeout))
        driver.get(profile_url)
        WebDriverWait(driver, min(max(timeout, 10), 45)).until(
            lambda d: len(d.find_element(By.TAG_NAME, "body").text) > 100
        )
        body_text = driver.find_element(By.TAG_NAME, "body").text
        page_date = _vanguard_page_month_end(body_text)

        script = """
            const url = arguments[0];
            const done = arguments[arguments.length - 1];
            fetch(url, {
                method: 'GET',
                credentials: 'include',
                headers: {'Accept': 'application/json,text/plain,*/*'}
            }).then(async r => {
                const text = await r.text();
                done({status: r.status, text: text});
            }).catch(e => done({status: 0, text: String(e)}));
        """
        result = driver.execute_async_script(script, api_url)
        status = int(result.get("status", 0))
        text = str(result.get("text", ""))
        if status == 200 and len(text) >= 100:
            import json
            payload = json.loads(text)
            return parse_vanguard_payload(payload, etf, source_date_hint=page_date)
        errors.append(f"selenium same-origin HTTP {status}, {len(text)} chars")
    except Exception as exc:
        errors.append(f"selenium: {type(exc).__name__}: {exc}")
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    raise HoldingsError(f"{etf}: Vanguard unavailable: " + " | ".join(errors[-8:]))


# ----------------------------- iShares / ACWI -----------------------------

def fetch_ishares(session: requests.Session, etf: str, timeout: int) -> FetchResult:
    response = http_get(
        session,
        ISHARES_ACWI_URL,
        timeout,
        headers={**HEADERS, "Accept": "text/csv,*/*;q=0.8", "Referer": "https://www.ishares.com/us/products/239600/ishares-msci-acwi-etf"},
    )
    text = response.text
    frame = sniff_csv(text, ["ticker", "name", "weight"])
    ticker_col = pick_col(frame, ["ticker"])
    exchange_col = pick_col(frame, ["exchange"])
    if ticker_col and exchange_col:
        frame = frame.copy()
        frame[ticker_col] = [qualify_ishares_ticker(t, e) for t, e in zip(frame[ticker_col], frame[exchange_col])]

    result_all = standardise(
        frame,
        ticker=ticker_col,
        name=pick_col(frame, ["name"]),
        weight=pick_col(frame, ["weight"]),
        shares=pick_col(frame, ["quantity", "shares"]),
        mval=pick_col(frame, ["market value"]),
        isin=pick_col(frame, ["isin"]),
        sedol=pick_col(frame, ["sedol"]),
    )
    market_value = pd.to_numeric(result_all["market_value"], errors="coerce")
    provider_weight = pd.to_numeric(result_all["weight"], errors="coerce").fillna(0.0)
    missing_weight = float(provider_weight.loc[market_value.isna()].abs().sum())
    if missing_weight > 0.05:
        raise HoldingsError(f"ACWI: market value missing for {missing_weight:.4f}% of reported weight")
    denominator = float(market_value.dropna().sum())
    if not math.isfinite(denominator) or denominator <= 0:
        raise HoldingsError(f"ACWI: invalid denominator {denominator}")
    valid = market_value.notna()
    result_all.loc[valid, "weight"] = market_value.loc[valid] / denominator * 100.0
    result = drop_nonsecurity_rows(result_all)
    return FetchResult(
        result,
        extract_date_from_text(text[:30000]) or iso_today(),
        "ishares_market_value_denominator",
    )


# --------------------------- State Street / SPDR --------------------------

def fetch_ssga(session: requests.Session, etf: str, timeout: int) -> FetchResult:
    response = http_get(session, SSGA_URL.format(ticker=etf.lower()), timeout)
    frame, source_date = excel_with_detected_header(response.content, required_tokens=("ticker", "weight"))
    result = standardise(
        frame,
        ticker=pick_col(frame, ["ticker"]),
        name=pick_col(frame, ["name", "security"]),
        weight=pick_col(frame, ["weight"]),
        shares=pick_col(frame, ["shares held", "shares", "quantity"]),
        mval=pick_col(frame, ["market value"]),
        isin=pick_col(frame, ["isin"]),
        sedol=pick_col(frame, ["sedol"]),
    )
    return FetchResult(drop_nonsecurity_rows(result), source_date or iso_today(), "provider_weight")


# ----------------------------- Schwab / SOXQ ------------------------------

def parse_compact_number(value: Any) -> float | None:
    if value is None:
        return None
    text = clean_text(value).upper().strip()
    if not text or text in {"-", "--", "N/A", "NA", "NONE"}:
        return None
    negative = text.startswith("(") and text.endswith(")")
    text = text.replace("$", "").replace(",", "").replace("%", "").strip("() ")
    multiplier = 1.0
    if text.endswith("K"):
        multiplier, text = 1_000.0, text[:-1]
    elif text.endswith("M"):
        multiplier, text = 1_000_000.0, text[:-1]
    elif text.endswith("B"):
        multiplier, text = 1_000_000_000.0, text[:-1]
    elif text.endswith("T"):
        multiplier, text = 1_000_000_000_000.0, text[:-1]
    try:
        number = float(text) * multiplier
    except ValueError:
        return None
    return -number if negative else number


def _schwab_source_date(page_text: str) -> str | None:
    match = re.search(
        r"(?i)\bAs\s+of(?:\s+close)?(?:\s+\d{1,2}:\d{2}\s*(?:am|pm)\s*ET)?"
        r"\s+(\d{1,2}/\d{1,2}/20\d{2})\b",
        page_text,
    )
    if match:
        return normalize_date_value(match.group(1))
    return extract_date_from_text(page_text)


def _schwab_rows_from_driver(driver: Any) -> list[dict[str, Any]]:
    from selenium.webdriver.common.by import By
    rows: list[dict[str, Any]] = []
    for row in driver.find_elements(By.CSS_SELECTOR, "#tthHoldingsTbody tr"):
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) < 5:
            continue
        symbol, name = clean_text(cells[0].text), clean_text(cells[1].text)
        if not symbol and not name:
            continue
        rows.append({
            "raw_ticker": symbol,
            "raw_name": name,
            "weight": to_float(cells[2].text),
            "shares": parse_compact_number(cells[3].text),
            "market_value": parse_compact_number(cells[4].text),
            "isin": "",
            "sedol": "",
        })
    return rows


def _schwab_collect_all_rows(driver: Any, timeout: int) -> list[dict[str, Any]]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    # Try Show 60 first.
    try:
        pagination = driver.find_element(By.ID, "PaginationContainer")
        for link in pagination.find_elements(By.TAG_NAME, "a"):
            if clean_text(link.text) == "60":
                driver.execute_script("arguments[0].click();", link)
                time.sleep(1)
                break
    except Exception:
        pass

    collected: dict[tuple[str, str], dict[str, Any]] = {}

    def absorb():
        for row in _schwab_rows_from_driver(driver):
            key = (clean_text(row["raw_ticker"]).upper(), normalized_name(row["raw_name"]))
            collected[key] = row

    absorb()
    for _ in range(5):
        try:
            pagination = driver.find_element(By.ID, "PaginationContainer")
            next_link = None
            for link in pagination.find_elements(By.TAG_NAME, "a"):
                if "next" in clean_text(link.text).lower():
                    parent_class = clean_text(link.find_element(By.XPATH, "..").get_attribute("class")).lower()
                    if "disabled" not in parent_class:
                        next_link = link
                    break
            if next_link is None:
                break
            before = set(collected)
            driver.execute_script("arguments[0].click();", next_link)
            WebDriverWait(driver, min(max(timeout, 5), 30)).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "#tthHoldingsTbody tr")) > 0
            )
            time.sleep(0.5)
            absorb()
            if set(collected) == before:
                break
        except Exception:
            break
    return list(collected.values())


def fetch_schwab_soxq(session: requests.Session, etf: str, timeout: int) -> FetchResult:
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as exc:
        raise HoldingsError("SOXQ: selenium required") from exc

    driver = None
    try:
        driver = _make_chrome_driver()
        driver.set_page_load_timeout(max(30, timeout))
        driver.get(SCHWAB_SOXQ_URL.format(ticker=etf))
        WebDriverWait(driver, min(max(timeout, 10), 45)).until(
            EC.presence_of_element_located((By.ID, "tthHoldingsTbody"))
        )
        page_text = driver.find_element(By.TAG_NAME, "body").text
        source_date = _schwab_source_date(page_text) or iso_today()
        rows = _schwab_collect_all_rows(driver, timeout)
    except Exception as exc:
        raise HoldingsError(f"SOXQ: Schwab fetch failed: {exc}") from exc
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    if len(rows) < MIN_HOLDINGS[etf]:
        raise HoldingsError(f"SOXQ: only {len(rows)} rows")
    result_all = pd.DataFrame(rows, columns=STD_COLS)
    result_all, precision = recompute_weights(result_all, require_all_market_values=False)
    result = drop_nonsecurity_rows(result_all)
    return FetchResult(result, source_date, f"schwab_wsod_{precision}")


# ------------------------------- Global X ---------------------------------

def fetch_globalx(session: requests.Session, etf: str, timeout: int) -> FetchResult:
    page = http_get(session, GLOBALX_PAGE_URL.format(ticker=etf.upper()), timeout)
    source_date = extract_date_from_text(page.text[:50000]) or iso_today()
    csv_url = (
        find_download_link(page.text, str(page.url), text_contains=("full holdings",), href_contains=(".csv",))
        or find_download_link(page.text, str(page.url), href_contains=(".csv",))
    )
    if not csv_url:
        raise HoldingsError(f"{etf}: Global X holdings CSV link not found")
    response = http_get(session, csv_url, timeout)
    parsed, download_date = parse_download(response, provider="Global X", source_date_hint=source_date)
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
    result_all, precision = recompute_weights(result_all, require_all_market_values=False)
    return FetchResult(drop_nonsecurity_rows(result_all), download_date or source_date, precision)


# ------------------------------ First Trust -------------------------------

def parse_firsttrust_dom(html_text: str) -> pd.DataFrame:
    root = lxml_html.fromstring(html_text)
    rows: list[dict[str, Any]] = []
    for row in root.xpath("//tr"):
        cells = [" ".join(clean_text(item) for item in cell.itertext()).strip() for cell in row.xpath("./th|./td")]
        cells = [cell for cell in cells if cell]
        if len(cells) < 6:
            continue
        weight, market_value, shares = to_float(cells[-1]), to_float(cells[-2]), to_float(cells[-3])
        if weight is None or market_value is None or shares is None:
            continue
        name, ticker = clean_text(cells[0]), clean_text(cells[1])
        if not ticker or ticker.lower() in {"identifier", "ticker"}:
            continue
        rows.append({
            "raw_ticker": ticker,
            "raw_name": name,
            "weight": weight,
            "shares": shares,
            "market_value": market_value,
            "isin": "",
            "sedol": "",
        })
    if not rows:
        raise HoldingsError("GRID: no First Trust rows found")
    return pd.DataFrame(rows, columns=STD_COLS)


def fetch_firsttrust(session: requests.Session, etf: str, timeout: int) -> FetchResult:
    page = http_get(session, FIRSTTRUST_URL.format(ticker=etf), timeout)
    source_date = extract_date_from_text(page.text[:50000]) or iso_today()
    result_all = parse_firsttrust_dom(page.text)
    result_all, precision = recompute_weights(result_all, require_all_market_values=True)
    return FetchResult(drop_nonsecurity_rows(result_all), source_date, precision)


# -------------------------------- VanEck ----------------------------------

def _parse_vaneck_download(response: Any, *, source_date_hint: str) -> FetchResult:
    parsed, source_date = parse_download(response, provider="VanEck", source_date_hint=source_date_hint)
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
    result_all, precision = recompute_weights(result_all, require_all_market_values=False)
    return FetchResult(drop_nonsecurity_rows(result_all), source_date or source_date_hint, precision)


def fetch_vaneck(session: requests.Session, etf: str, timeout: int) -> FetchResult:
    errors: list[str] = []
    source_date = iso_today()
    try:
        response = session.get(
            VANECK_XLSX_URL,
            headers={**HEADERS, "Referer": VANECK_PAGE_URL, "Accept": DOWNLOAD_ACCEPT},
            timeout=(10, timeout),
            allow_redirects=True,
        )
        if response.status_code == 200 and len(response.content) >= 100:
            return _parse_vaneck_download(response, source_date_hint=source_date)
        errors.append(f"direct XLSX HTTP {response.status_code}, {len(response.content)} bytes")
    except Exception as exc:
        errors.append(f"direct XLSX: {exc}")

    try:
        page = http_get(session, VANECK_PAGE_URL, timeout)
        source_date = extract_date_from_text(page.text[:50000]) or source_date
        xls_url = (
            find_download_link(page.text, str(page.url), text_contains=("download",), href_contains=("holding",))
            or find_download_link(page.text, str(page.url), href_contains=(".xls",))
        )
        if not xls_url:
            raise HoldingsError("holdings XLS link not found")
        return _parse_vaneck_download(http_get(session, xls_url, timeout), source_date_hint=source_date)
    except Exception as exc:
        errors.append(f"page discovery: {exc}")
    raise HoldingsError(f"{etf}: " + " | ".join(errors))


# ------------------------ Canonicalization / output -----------------------

def canonical_ticker(raw: str) -> str:
    ticker = clean_text(raw).upper()
    ticker = re.sub(r"\s+(US|UN|UW|UQ|UR|UF)$", "", ticker)
    ticker = ticker.replace("/", "-")
    ticker = re.sub(r"\s+", "-", ticker)
    ticker = re.sub(r"-+", "-", ticker)
    return ticker.strip("-")


def normalized_name(raw: str) -> str:
    text = unicodedata.normalize("NFKD", clean_text(raw)).encode("ascii", "ignore").decode().upper()
    text = re.sub(r"[^A-Z0-9& ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def identity_key(row: pd.Series) -> str:
    isin = clean_text(row.get("isin", "")).upper()
    sedol = clean_text(row.get("sedol", "")).upper()
    ticker = canonical_ticker(row.get("raw_ticker", ""))
    name = normalized_name(row.get("raw_name", ""))
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", isin):
        return "ISIN:" + isin
    if re.fullmatch(r"[A-Z0-9]{7}", sedol):
        return "SEDOL:" + sedol
    return f"TK:{ticker}|{name}"


def canonicalize_across_etfs(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["_identity"] = out.apply(identity_key, axis=1)
    out["_ticker"] = out["raw_ticker"].map(canonical_ticker)
    out["_name"] = out["raw_name"].map(clean_text)
    ticker_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    for identity, group in out.groupby("_identity", sort=False):
        tickers = [v for v in group["_ticker"] if v]
        names = [v for v in group["_name"] if v]
        ticker_map[str(identity)] = str(pd.Series(tickers).value_counts().index[0]) if tickers else ""
        if names:
            counts = pd.Series(names).value_counts()
            top = int(counts.iloc[0])
            tied = [str(name) for name, count in counts.items() if int(count) == top]
            tied.sort(key=lambda v: (v.isupper(), len(v), v.upper()))
            name_map[str(identity)] = tied[0]
        else:
            name_map[str(identity)] = ticker_map[str(identity)]
    out["canonical_ticker"] = out["_identity"].map(lambda v: ticker_map[str(v)])
    out["canonical_name"] = out["_identity"].map(lambda v: name_map[str(v)])
    return out


def validate_live_frame(etf: str, frame: pd.DataFrame, previous_count: int | None) -> None:
    if frame.empty:
        raise HoldingsError(f"{etf}: no holdings")
    required = MIN_HOLDINGS[etf]
    if previous_count and previous_count >= required:
        required = max(required, math.floor(previous_count * MIN_PREVIOUS_RATIO))
    if len(frame) < required:
        raise HoldingsError(f"{etf}: only {len(frame)} rows; required at least {required}")
    weights = pd.to_numeric(frame["weight"], errors="coerce")
    numeric_count = int(weights.notna().sum())
    if numeric_count < math.floor(len(frame) * 0.97):
        raise HoldingsError(f"{etf}: only {numeric_count}/{len(frame)} rows have numeric weights")
    if weights.dropna().lt(-0.05).any():
        raise HoldingsError(f"{etf}: materially negative security weight")
    total = float(weights.sum(skipna=True))
    if not (MIN_TOTAL_WEIGHT <= total <= MAX_TOTAL_WEIGHT):
        raise HoldingsError(f"{etf}: weight sum {total:.6f}% outside [{MIN_TOTAL_WEIGHT}, {MAX_TOTAL_WEIGHT}]")


def load_previous(path: Path) -> dict[str, pd.DataFrame]:
    if not path.exists():
        return {}
    try:
        previous = pd.read_csv(path)
    except Exception as exc:
        print(f"WARNING: could not read previous CSV: {exc}", file=sys.stderr)
        return {}
    if list(previous.columns) != OUTPUT_COLS:
        print("WARNING: previous CSV schema mismatch; ignoring previous file", file=sys.stderr)
        return {}
    return {
        str(ticker).upper(): group[OUTPUT_COLS].copy().reset_index(drop=True)
        for ticker, group in previous.groupby("fund_ticker", sort=False)
    }


def sanitize_previous(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame[OUTPUT_COLS].copy()
    out["holding_ticker"] = out["holding_ticker"].map(clean_text)
    out["holding_name"] = out["holding_name"].map(clean_text)
    keep = [not is_nonsecurity(t, n) for t, n in zip(out["holding_ticker"], out["holding_name"])]
    out = out.loc[keep].reset_index(drop=True)
    out["rank"] = range(1, len(out) + 1)
    return out


def validate_output(etf: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        raise HoldingsError(f"{etf}: output rows empty")
    if len(frame) < MIN_HOLDINGS[etf]:
        raise HoldingsError(f"{etf}: output has only {len(frame)} rows")
    total = float(pd.to_numeric(frame["weight"], errors="coerce").sum(skipna=True))
    if not (MIN_TOTAL_WEIGHT <= total <= MAX_TOTAL_WEIGHT):
        raise HoldingsError(f"{etf}: output weight sum {total:.6f}% invalid")
    if etf == "VGT":
        dates = pd.to_datetime(frame["source_date"], errors="coerce").dropna()
        if dates.empty or len({d.date().isoformat() for d in dates}) != 1:
            raise HoldingsError("VGT: invalid/mixed source_date")
        validate_vanguard_source_date(dates.iloc[0].date().isoformat())


def normalized_output(
    etf: str,
    canonical_frame: pd.DataFrame,
    source_date: str,
    retrieved_at: datetime,
) -> pd.DataFrame:
    fund = canonical_frame[canonical_frame["etf"].eq(etf)].copy()
    for col in ("weight", "shares", "market_value"):
        fund[col] = pd.to_numeric(fund[col], errors="coerce")
    fund = fund.sort_values("weight", ascending=False, na_position="last").reset_index(drop=True)
    out = pd.DataFrame({
        "source_date": source_date,
        "retrieved_at_utc": iso_utc(retrieved_at),
        "fund_ticker": etf,
        "provider": PROVIDER_LABELS[etf],
        "rank": range(1, len(fund) + 1),
        "holding_ticker": fund["canonical_ticker"],
        "holding_name": fund["canonical_name"],
        "weight": fund["weight"],
        "shares_held": fund["shares"],
        "market_value_usd": fund["market_value"],
    })
    return out[OUTPUT_COLS]


def write_csv_atomic(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8-sig", float_format="%.15g")
        if not temporary.exists() or temporary.stat().st_size == 0:
            raise HoldingsError(f"Generated empty CSV: {temporary}")
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


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
    "SOXQ": fetch_schwab_soxq,
}
DEFAULT_ETFS = ["VGT", "ACWI", "XLF", "XLI", "XLC", "PPH", "MLPX", "GRID", "SOXQ"]


def fetch_all_live(
    session: requests.Session,
    etfs: list[str],
    previous: dict[str, pd.DataFrame],
    timeout: int,
) -> tuple[dict[str, pd.DataFrame], dict[str, str], dict[str, str], dict[str, str]]:
    live: dict[str, pd.DataFrame] = {}
    source_dates: dict[str, str] = {}
    precision_methods: dict[str, str] = {}
    failures: dict[str, str] = {}

    for etf in etfs:
        print(f"[{etf}] Fetching {PROVIDER_LABELS[etf]}...", file=sys.stderr)
        try:
            result = FETCHERS[etf](session, etf, timeout)
            frame = result.frame.copy()

            # VGT CRITICAL CHANGE:
            # No drift_vgt_live_snapshot(). Vanguard provider weights are used
            # exactly as published.
            previous_count = len(previous[etf]) if etf in previous else None
            validate_live_frame(etf, frame, previous_count)
            if etf == "VGT":
                validate_vanguard_source_date(result.source_date)
            frame["etf"] = etf
            live[etf] = frame
            source_dates[etf] = result.source_date
            precision_methods[etf] = result.precision_method
            print(
                f"[{etf}] OK: {len(frame)} rows, "
                f"{pd.to_numeric(frame['weight'], errors='coerce').sum():.8f}% "
                f"source_date={result.source_date} ({result.precision_method})",
                file=sys.stderr,
            )
        except Exception as exc:
            failures[etf] = str(exc)
            print(f"[{etf}] LIVE FAILED: {exc}", file=sys.stderr)

    return live, source_dates, precision_methods, failures


def run(etfs: list[str], out_dir: Path, combined_name: str, timeout: int) -> int:
    combined_path = out_dir / combined_name
    previous = load_previous(combined_path)

    with build_session() as session:
        live, source_dates, precision_methods, live_failures = fetch_all_live(
            session, etfs, previous, timeout
        )

    retrieved_at = utc_now()
    canonical_live: pd.DataFrame | None = None
    if live:
        stacked = pd.concat([live[e] for e in etfs if e in live], ignore_index=True)
        canonical_live = canonicalize_across_etfs(stacked)

    final_by_etf: dict[str, pd.DataFrame] = {}
    source_status: dict[str, str] = {}
    fatal: dict[str, str] = {}

    for etf in etfs:
        if etf in live and canonical_live is not None:
            output = normalized_output(etf, canonical_live, source_dates[etf], retrieved_at)
            validate_output(etf, output)
            final_by_etf[etf] = output
            source_status[etf] = f"{PROVIDER_LABELS[etf]} / {precision_methods[etf]}"
            continue

        if etf not in previous:
            fatal[etf] = f"live failed ({live_failures.get(etf, 'unknown')}); no last-known-good rows"
            continue

        try:
            fallback = sanitize_previous(previous[etf].copy())
            validate_output(etf, fallback)

            # CRITICAL VGT CHANGE:
            # Preserve last official Vanguard snapshot EXACTLY. Never Yahoo-drift
            # it. validate_output() also rejects the old fake non-month-end
            # source_date such as 2026-08-26.
            fallback_mode = "last-known-good"
            if etf == "VGT":
                fallback_mode = "last official Vanguard month-end snapshot (unchanged)"

            final_by_etf[etf] = fallback[OUTPUT_COLS]
            source_status[etf] = fallback_mode
            print(f"[{etf}] Using {fallback_mode}; source_date preserved", file=sys.stderr)
        except Exception as exc:
            fatal[etf] = (
                f"live failed ({live_failures.get(etf, 'unknown')}); "
                f"fallback failed ({exc})"
            )

    if fatal:
        print("\nUpdate aborted. Existing combined CSV was NOT replaced.", file=sys.stderr)
        for etf, reason in fatal.items():
            print(f"  - {etf}: {reason}", file=sys.stderr)
        return 1

    combined = pd.concat([final_by_etf[e] for e in etfs], ignore_index=True)
    if list(combined.columns) != OUTPUT_COLS:
        print(f"ERROR: output schema changed: {list(combined.columns)}", file=sys.stderr)
        return 1
    expected_funds = set(etfs)
    actual_funds = set(combined["fund_ticker"].dropna().astype(str).str.upper().unique())
    if actual_funds != expected_funds:
        print(f"ERROR: ETF set mismatch. Expected={expected_funds}, Actual={actual_funds}", file=sys.stderr)
        return 1
    for etf, group in combined.groupby("fund_ticker", sort=False):
        validate_output(str(etf), group)

    write_csv_atomic(combined, combined_path)
    print(f"\nPublished {len(combined):,} rows across {len(etfs)} ETFs -> {combined_path}", file=sys.stderr)
    print("Sources used:", file=sys.stderr)
    for etf in etfs:
        print(f"  - {etf}: {source_status[etf]}", file=sys.stderr)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tickers", nargs="*", help="Optional ETF tickers; defaults to production basket")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--combined-name", default=DEFAULT_COMBINED_NAME)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if Path(args.combined_name).name != args.combined_name:
        print("ERROR: --combined-name must be a filename, not a path", file=sys.stderr)
        return 2
    if args.timeout <= 0:
        print("ERROR: --timeout must be > 0", file=sys.stderr)
        return 2
    etfs = (
        [ticker.strip().upper() for ticker in args.tickers if ticker.strip()]
        if args.tickers
        else DEFAULT_ETFS.copy()
    )
    etfs = list(dict.fromkeys(etfs))
    unknown = [e for e in etfs if e not in FETCHERS]
    if unknown:
        print(f"ERROR: unknown ETF(s): {unknown}. Known={list(FETCHERS)}", file=sys.stderr)
        return 2
    return run(etfs, args.out_dir, args.combined_name, args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
