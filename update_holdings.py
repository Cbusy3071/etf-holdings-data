"""
Provider reliability patch for the existing ETF holdings pipeline.

This file imports the existing `update_holdings.py` and replaces only the
provider/fallback paths that were failing in GitHub Actions: VGT, ACWI and SOXQ.
The other six ETF paths remain unchanged.

What this fixes
---------------
1. Invesco HTTP 406 / bot filtering:
   - tries the existing requests session first;
   - retries with curl_cffi using a Chrome TLS/browser fingerprint;
   - tries both legacy and /en/ Invesco export URL variants;
   - warms the browser-like session on the Invesco home/product pages;
   - discovers export-like URLs from returned HTML when possible.

2. Broken SOXQ fallback repricing:
   - reprices EVERY SOXQ holding from the last official snapshot when the
     official Invesco fetch is unavailable;
   - does not restrict repricing to rows whose old market value is blank;
   - adjusts snapshot share counts for stock splits occurring after the
     official snapshot;
   - recomputes current estimated weights from shares x latest price;
   - preserves the official source_date so downstream users can still see the
     composition snapshot is stale;
   - updates retrieved_at_utc to the time the drift estimate was calculated.

3. Vanguard/VGT intermittent API failures:
   - keeps the normal Vanguard JSON route first;
   - retries the Vanguard API with curl_cffi Chrome impersonation if the
     requests response is HTML/non-JSON or otherwise rejected;
   - if Vanguard is still unavailable, reprices the entire last-known Vanguard
     share snapshot using Yahoo, including post-snapshot stock splits;
   - preserves the official composition source_date while refreshing
     retrieved_at_utc.

4. iShares/ACWI precision failure:
   - uses iShares' own Market Value column directly;
   - computes the denominator across the complete iShares file BEFORE removing
     cash/derivative rows;
   - tolerates missing Market Value only for immaterial 0.00%-weight rows;
   - then publishes high-precision security weights from market value / fund
     denominator instead of rejecting an otherwise valid iShares file.

5. Stale-snapshot safety:
   - warns when drifted VGT/SOXQ share snapshots become old;
   - refuses to drift SOXQ beyond 30 days and VGT beyond 45 days.

Run it with the same CLI arguments as update_holdings.py, e.g.:

    python update_holdings_fixed.py \
      --out-dir data \
      --combined-name ETF_Holdings_Latest.csv \
      VGT ACWI XLF XLI XLC PPH MLPX GRID SOXQ

Dependencies (in addition to the existing pipeline):
    pip install curl_cffi
"""

from __future__ import annotations

import math
import re
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import pandas as pd

import update_holdings as base


SOXQ_MAX_DRIFT_DAYS = 30
SOXQ_WARN_DRIFT_DAYS = 7
VGT_MAX_DRIFT_DAYS = 45
VGT_WARN_DRIFT_DAYS = 10
ACWI_MAX_SOURCE_AGE_DAYS = 5
ACWI_WARN_SOURCE_AGE_DAYS = 2

INVESCO_PAGE_URLS = [
    base.INVESCO_PAGE_URL,
    base.INVESCO_PAGE_URL + "?audienceType=Investor",
]

INVESCO_EXPORT_URLS = [
    # Existing/legacy route.
    (
        "https://www.invesco.com/us/financial-products/"
        "etfs/holdings/main/holdings/0"
        "?audienceType=Investor&action=download&ticker={ticker}"
    ),
    # Current site URLs commonly include /en/; try this variant too.
    (
        "https://www.invesco.com/us/en/financial-products/"
        "etfs/holdings/main/holdings/0"
        "?audienceType=Investor&action=download&ticker={ticker}"
    ),
]

HTML_ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,*/*;q=0.8"
)

DOWNLOAD_ACCEPT = (
    "text/csv,application/csv,application/vnd.ms-excel,"
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
    "application/octet-stream;q=0.9,*/*;q=0.8"
)


# Keep the original fallback helper for every ETF other than SOXQ.
_original_fill_output_market_values_from_yahoo = (
    base.fill_output_market_values_from_yahoo
)

_original_fetch_vanguard = base.fetch_vanguard

# iShares currently uses this exact label for KOSDAQ listings. The base file
# has used other variants historically, so accept both without changing the
# output schema.
base.ISHARES_EXCHANGE_SUFFIX.update(
    {
        "Korea Exchange (Kosdaq)": ".KQ",
        "Kosdaq Market": ".KQ",
    }
)


def _response_ok(response: Any) -> bool:
    return (
        response is not None
        and getattr(response, "status_code", None) == 200
        and len(getattr(response, "content", b"")) >= 100
    )


def _response_summary(response: Any) -> str:
    if response is None:
        return "no response"

    status = getattr(response, "status_code", "?")
    content = getattr(response, "content", b"") or b""
    final_url = getattr(response, "url", "")
    content_type = (
        getattr(response, "headers", {})
        .get("content-type", "")
    )

    return (
        f"HTTP {status}, {len(content)} bytes, "
        f"content-type={content_type!r}, url={final_url}"
    )


def _discover_invesco_urls(html_text: str, base_url: str) -> list[str]:
    """Best-effort extraction of export/holdings URLs from Invesco HTML."""

    found: list[str] = []

    # First use the existing DOM link helper.
    for text_tokens, href_tokens in (
        (("export",), ()),
        (("download",), ("holding",)),
        ((), ("holdings",)),
    ):
        try:
            url = base.find_download_link(
                html_text,
                base_url,
                text_contains=text_tokens,
                href_contains=href_tokens,
            )
        except Exception:
            url = None

        if url:
            found.append(url)

    # Invesco can embed endpoints inside script/JSON rather than anchors.
    # Search both absolute and escaped/relative URL-looking strings.
    patterns = [
        r'https?://[^"\'<>\\\s]+(?:holding|holdings)[^"\'<>\\\s]*',
        r'(?P<url>/us/(?:en/)?financial-products/etfs/holdings/[^"\'<>\\\s]+)',
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, html_text, flags=re.IGNORECASE):
            value = (
                match.groupdict().get("url")
                if match.groupdict()
                else match.group(0)
            )
            value = (value or "").replace("\\/", "/").replace("&amp;", "&")
            if value:
                found.append(urljoin(base_url, value))

    # Prefer URLs that look like an actual download/export endpoint.
    def score(url: str) -> tuple[int, int]:
        lowered = url.lower()
        points = 0
        if "action=download" in lowered:
            points += 5
        if "ticker=" in lowered:
            points += 3
        if "holding" in lowered:
            points += 2
        if lowered.endswith((".csv", ".xls", ".xlsx")):
            points += 2
        return (points, -len(url))

    return sorted(
        dict.fromkeys(found),
        key=score,
        reverse=True,
    )


def _parse_invesco_download(
    response: Any,
    *,
    source_date_hint: str,
) -> base.FetchResult:
    parsed, download_date = base.parse_downloaded_holdings(
        response,
        provider="Invesco",
        source_date_hint=source_date_hint,
    )

    result_all = base.standardise(
        parsed,
        ticker=base.pick_col(
            parsed,
            ["holding ticker", "ticker", "symbol"],
        ),
        name=base.pick_col(
            parsed,
            ["holding name", "name", "security", "description", "holding"],
        ),
        weight=base.pick_col(
            parsed,
            ["weight", "% of fund", "% of net", "%"],
        ),
        shares=base.pick_col(
            parsed,
            ["shares", "shares held", "quantity"],
        ),
        mval=base.pick_col(
            parsed,
            ["market value", "marketvalue"],
        ),
        isin=base.pick_col(
            parsed,
            ["isin", "security identifier"],
        ),
        sedol=base.pick_col(
            parsed,
            ["sedol"],
        ),
    )

    result_all, precision = base.maybe_recompute_from_market_value(
        result_all
    )
    result = base.drop_nonsecurity_rows(result_all)

    # Validate here so a 200 response containing a consent/interstitial page or
    # malformed export cannot be accepted as holdings.
    base.validate_live_frame(
        "SOXQ",
        result,
        previous_count=None,
    )

    return base.FetchResult(
        result,
        download_date or source_date_hint,
        precision,
    )


def _try_invesco_with_requests(
    session: Any,
    etf: str,
    timeout: int,
    errors: list[str],
) -> base.FetchResult | None:
    source_date = base.iso_today()
    candidates: list[str] = []

    for page_url in INVESCO_PAGE_URLS:
        try:
            page = session.get(
                page_url,
                headers={
                    **base.HEADERS,
                    "Accept": HTML_ACCEPT,
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Upgrade-Insecure-Requests": "1",
                },
                timeout=(10, timeout),
                allow_redirects=True,
            )

            if _response_ok(page):
                source_date = (
                    base.extract_date_from_text(page.text[:50000])
                    or source_date
                )
                candidates.extend(
                    _discover_invesco_urls(page.text, str(page.url))
                )
                break

            errors.append(
                f"requests product page {page_url}: {_response_summary(page)}"
            )

        except Exception as exc:
            errors.append(
                f"requests product page {page_url}: {type(exc).__name__}: {exc}"
            )

    candidates.extend(
        url.format(ticker=etf)
        for url in INVESCO_EXPORT_URLS
    )

    for url in dict.fromkeys(candidates):
        try:
            response = session.get(
                url,
                headers={
                    **base.HEADERS,
                    "Referer": INVESCO_PAGE_URLS[0],
                    "Accept": DOWNLOAD_ACCEPT,
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                },
                timeout=(10, timeout),
                allow_redirects=True,
            )

            if not _response_ok(response):
                errors.append(
                    f"requests export {url}: {_response_summary(response)}"
                )
                continue

            return _parse_invesco_download(
                response,
                source_date_hint=source_date,
            )

        except Exception as exc:
            errors.append(
                f"requests export {url}: {type(exc).__name__}: {exc}"
            )

    return None


def _try_invesco_with_curl_cffi(
    etf: str,
    timeout: int,
    errors: list[str],
) -> base.FetchResult | None:
    """Retry Invesco using a Chrome-like TLS/browser fingerprint."""

    try:
        from curl_cffi import requests as cffi_requests
    except Exception as exc:
        errors.append(
            f"curl_cffi import failed: {type(exc).__name__}: {exc}"
        )
        return None

    source_date = base.iso_today()
    candidates: list[str] = []

    try:
        browser = cffi_requests.Session(
            impersonate="chrome",
            headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )
    except Exception as exc:
        errors.append(
            f"curl_cffi session creation failed: {type(exc).__name__}: {exc}"
        )
        return None

    try:
        # Warm the session. Failure is non-fatal; the product page/export may
        # still work directly.
        try:
            browser.get(
                "https://www.invesco.com/us/",
                headers={"Accept": HTML_ACCEPT},
                timeout=timeout,
                allow_redirects=True,
            )
        except Exception:
            pass

        for page_url in INVESCO_PAGE_URLS:
            try:
                page = browser.get(
                    page_url,
                    headers={
                        "Accept": HTML_ACCEPT,
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "same-origin",
                    },
                    timeout=timeout,
                    allow_redirects=True,
                )

                if _response_ok(page):
                    source_date = (
                        base.extract_date_from_text(page.text[:50000])
                        or source_date
                    )
                    candidates.extend(
                        _discover_invesco_urls(page.text, str(page.url))
                    )
                    break

                errors.append(
                    f"curl_cffi product page {page_url}: {_response_summary(page)}"
                )

            except Exception as exc:
                errors.append(
                    f"curl_cffi product page {page_url}: "
                    f"{type(exc).__name__}: {exc}"
                )

        candidates.extend(
            url.format(ticker=etf)
            for url in INVESCO_EXPORT_URLS
        )

        for url in dict.fromkeys(candidates):
            try:
                response = browser.get(
                    url,
                    headers={
                        "Referer": INVESCO_PAGE_URLS[0],
                        "Accept": DOWNLOAD_ACCEPT,
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "same-origin",
                    },
                    timeout=timeout,
                    allow_redirects=True,
                )

                if not _response_ok(response):
                    errors.append(
                        f"curl_cffi export {url}: {_response_summary(response)}"
                    )
                    continue

                return _parse_invesco_download(
                    response,
                    source_date_hint=source_date,
                )

            except Exception as exc:
                errors.append(
                    f"curl_cffi export {url}: {type(exc).__name__}: {exc}"
                )

    finally:
        try:
            browser.close()
        except Exception:
            pass

    return None



def _walk_dict_lists_local(value: Any):
    if isinstance(value, list):
        if value and all(isinstance(item, dict) for item in value):
            yield value
        for item in value:
            yield from _walk_dict_lists_local(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _walk_dict_lists_local(item)


def _score_vanguard_rows_local(rows: list[dict[str, Any]]) -> int:
    score = 0
    for row in rows[:20]:
        keys = {str(key).lower() for key in row}
        if {"ticker", "tickersymbol", "symbol"} & keys:
            score += 3
        if {"sharesheld", "shares", "quantity"} & keys:
            score += 3
        if {"marketvalue", "marketval"} & keys:
            score += 2
        if {"longname", "shortname", "name", "securityname"} & keys:
            score += 1
    return score


def _parse_vanguard_payload(payload: Any, etf: str) -> base.FetchResult:
    candidates = list(_walk_dict_lists_local(payload))
    if not candidates:
        raise base.HoldingsError(
            f"{etf}: no holdings list found in Vanguard JSON"
        )

    entities = max(candidates, key=_score_vanguard_rows_local)
    if _score_vanguard_rows_local(entities) <= 0:
        raise base.HoldingsError(
            f"{etf}: Vanguard JSON did not contain recognizable holdings"
        )

    rows: list[dict[str, Any]] = []
    for entity in entities:
        rows.append(
            {
                "raw_ticker": base.clean_text(
                    entity.get("ticker")
                    or entity.get("tickerSymbol")
                    or entity.get("symbol")
                    or ""
                ),
                "raw_name": base.clean_text(
                    entity.get("longName")
                    or entity.get("shortName")
                    or entity.get("name")
                    or entity.get("securityName")
                    or ""
                ),
                "weight": base.to_float(
                    entity.get("percentWeight")
                    or entity.get("weight")
                    or entity.get("percent")
                ),
                "shares": base.to_float(
                    entity.get("sharesHeld")
                    or entity.get("shares")
                    or entity.get("quantity")
                ),
                "market_value": base.to_float(
                    entity.get("marketValue")
                    or entity.get("marketVal")
                    or entity.get("value")
                ),
                "isin": base.clean_text(entity.get("isin") or ""),
                "sedol": base.clean_text(entity.get("sedol") or ""),
            }
        )

    frame = pd.DataFrame(rows, columns=base.STD_COLS)
    frame = base.drop_nonsecurity_rows(frame)

    return base.FetchResult(
        frame,
        base.recursive_find_date(payload) or base.iso_today(),
        "vanguard_snapshot",
    )


def fetch_vanguard_fixed(
    session: Any,
    etf: str,
    timeout: int,
) -> base.FetchResult:
    """
    Vanguard normally works with requests. If the API intermittently returns
    HTML/non-JSON to a GitHub runner, retry it using a Chrome TLS fingerprint.
    """

    try:
        return _original_fetch_vanguard(session, etf, timeout)
    except Exception as first_exc:
        print(
            f"[{etf}] Standard Vanguard request failed ({first_exc}); "
            "retrying with curl_cffi Chrome impersonation...",
            file=sys.stderr,
        )

    try:
        from curl_cffi import requests as cffi_requests
    except Exception as exc:
        raise base.HoldingsError(
            f"{etf}: Vanguard requests failed and curl_cffi import failed: {exc}"
        ) from exc

    errors: list[str] = []
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
                f"https://investor.vanguard.com/investment-products/etfs/profile/{etf.lower()}",
                timeout=timeout,
                allow_redirects=True,
            )
        except Exception:
            pass

        for template in base.VANGUARD_URLS:
            url = template.format(t=etf.lower())
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
                if not _response_ok(response):
                    errors.append(
                        f"{url}: {_response_summary(response)}"
                    )
                    continue

                try:
                    payload = response.json()
                except Exception as exc:
                    errors.append(
                        f"{url}: HTTP 200 but non-JSON response: {exc}"
                    )
                    continue

                result = _parse_vanguard_payload(payload, etf)
                print(
                    f"[{etf}] curl_cffi Vanguard retry succeeded.",
                    file=sys.stderr,
                )
                return result

            except Exception as exc:
                errors.append(
                    f"{url}: {type(exc).__name__}: {exc}"
                )
    finally:
        try:
            browser.close()
        except Exception:
            pass

    raise base.HoldingsError(
        f"{etf}: Vanguard requests + curl_cffi routes failed: "
        + " | ".join(errors[-6:])
    )


def fetch_ishares_fixed(
    session: Any,
    etf: str,
    timeout: int,
) -> base.FetchResult:
    """
    Parse ACWI from the official iShares CSV and derive high-precision weights
    from Market Value directly.

    The old generic path required every parsed row to have a numeric market
    value before recomputation. A single immaterial blank/metadata row therefore
    forced the whole ETF onto stale last-known-good data. Here we allow missing
    MV only where the provider weight itself is effectively zero.
    """

    _, response = base.try_urls(
        session,
        base.ISHARES_ACWI_URLS,
        timeout,
    )
    text = response.text

    frame, _ = base.sniff_csv(
        text,
        ["ticker", "name", "weight"],
    )

    ticker_col = base.pick_col(frame, ["ticker"])
    exchange_col = base.pick_col(frame, ["exchange"])
    weight_col = base.pick_col(frame, ["weight"])
    market_value_col = base.pick_col(frame, ["market value"])

    if not ticker_col or not weight_col or not market_value_col:
        raise base.HoldingsError(
            "ACWI: required iShares columns missing; "
            f"columns={list(frame.columns)}"
        )

    frame = frame.copy()

    if exchange_col:
        frame[ticker_col] = [
            base.qualify_ishares_ticker(ticker, exchange)
            for ticker, exchange in zip(
                frame[ticker_col],
                frame[exchange_col],
            )
        ]

    # Compute the fund denominator from the COMPLETE iShares file first.
    # This preserves the effect of cash (including negative USD cash) while
    # avoiding the old generic all-rows-must-be-perfect failure mode.
    raw_market_values = frame[market_value_col].map(base.to_float)
    raw_provider_weights = frame[weight_col].map(base.to_float)

    missing_mv = raw_market_values.isna()
    materially_missing = (
        missing_mv
        & (
            raw_provider_weights.isna()
            | raw_provider_weights.abs().gt(0.005)
        )
    )

    if materially_missing.any():
        bad_cols = [
            column
            for column in (
                ticker_col,
                base.pick_col(frame, ["name"]),
                weight_col,
                market_value_col,
            )
            if column
        ]
        bad = frame.loc[
            materially_missing,
            bad_cols,
        ].head(10)
        raise base.HoldingsError(
            "ACWI: material rows are missing iShares Market Value; "
            f"bad rows={bad.to_dict(orient='records')}"
        )

    # Only immaterial provider-0.00% rows may have blank MV.
    raw_market_values = raw_market_values.fillna(0.0)

    denominator = float(raw_market_values.sum())
    if not math.isfinite(denominator) or denominator <= 0:
        raise base.HoldingsError(
            f"ACWI: invalid iShares market-value denominator {denominator}"
        )

    frame["__market_value_clean"] = raw_market_values
    frame["__weight_precise"] = (
        raw_market_values / denominator * 100.0
    )

    # ACWI is an equity ETF. iShares also includes cash and futures rows in the
    # CSV. They belong in the denominator but should not become holdings rows.
    asset_class_col = base.pick_col(frame, ["asset class"])
    if asset_class_col:
        asset_class = (
            frame[asset_class_col]
            .map(base.clean_text)
            .str.lower()
        )
        security_mask = ~asset_class.str.contains(
            r"cash|future|derivative",
            regex=True,
            na=False,
        )
        security_frame = frame.loc[security_mask].copy()
    else:
        security_frame = frame.copy()

    result = base.standardise(
        security_frame,
        ticker=ticker_col,
        name=base.pick_col(security_frame, ["name"]),
        weight="__weight_precise",
        shares=base.pick_col(security_frame, ["quantity", "shares"]),
        mval="__market_value_clean",
        isin=base.pick_col(security_frame, ["isin"]),
        sedol=base.pick_col(security_frame, ["sedol"]),
    )

    # Final defensive cleanup catches any provider-specific cash labels not
    # identified through Asset Class.
    result = base.drop_nonsecurity_rows(result)

    base.validate_live_frame(
        "ACWI",
        result,
        previous_count=None,
    )

    missing_count = int(missing_mv.sum())
    if missing_count:
        print(
            f"[ACWI] Accepted {missing_count} immaterial 0.00%-weight "
            "row(s) with blank Market Value.",
            file=sys.stderr,
        )

    source_date_text = (
        base.extract_date_from_text(text[:20000])
        or base.iso_today()
    )

    try:
        source_date_value = datetime.fromisoformat(
            source_date_text
        ).date()
    except ValueError as exc:
        raise base.HoldingsError(
            f"ACWI: invalid iShares source date {source_date_text!r}"
        ) from exc

    source_age = (
        datetime.now(timezone.utc).date()
        - source_date_value
    ).days

    if source_age < 0:
        raise base.HoldingsError(
            f"ACWI: iShares source date {source_date_value} is in the future"
        )

    if source_age > ACWI_MAX_SOURCE_AGE_DAYS:
        raise base.HoldingsError(
            "ACWI: iShares daily holdings file is unexpectedly stale: "
            f"{source_date_value} ({source_age} days old)"
        )

    if source_age > ACWI_WARN_SOURCE_AGE_DAYS:
        print(
            "[ACWI] WARNING: official iShares holdings file is "
            f"{source_age} days old ({source_date_value}).",
            file=sys.stderr,
        )

    return base.FetchResult(
        result,
        source_date_text,
        "ishares_market_value_recomputed",
    )


def _snapshot_date_from_output_for_label(
    frame: pd.DataFrame,
    label: str,
) -> date:
    values = (
        frame["source_date"]
        .dropna()
        .astype(str)
        .map(str.strip)
    )
    if values.empty:
        raise base.HoldingsError(
            f"{label}: fallback has no source_date; cannot safely drift weights"
        )

    unique = list(dict.fromkeys(values.tolist()))
    if len(unique) != 1:
        raise base.HoldingsError(
            f"{label}: fallback contains multiple source dates: {unique[:5]}"
        )

    try:
        return datetime.fromisoformat(unique[0]).date()
    except ValueError as exc:
        raise base.HoldingsError(
            f"{label}: invalid fallback source_date {unique[0]!r}"
        ) from exc


def _reprice_vgt_snapshot(
    frame: pd.DataFrame,
    *,
    retrieved_at: datetime,
) -> pd.DataFrame:
    """
    Reuse the base pipeline's split-aware Vanguard drift logic on the previous
    output rows when today's Vanguard API call fails.
    """

    output = frame.copy().reset_index(drop=True)
    snapshot_date = _snapshot_date_from_output_for_label(output, "VGT")
    today = datetime.now(timezone.utc).date()
    age_days = (today - snapshot_date).days

    if age_days < 0:
        raise base.HoldingsError(
            f"VGT: source_date {snapshot_date} is in the future"
        )
    if age_days > VGT_MAX_DRIFT_DAYS:
        raise base.HoldingsError(
            "VGT: official share snapshot is too old to drift safely: "
            f"{snapshot_date} ({age_days} days old; maximum "
            f"{VGT_MAX_DRIFT_DAYS})"
        )
    if age_days > VGT_WARN_DRIFT_DAYS:
        print(
            "[VGT] WARNING: official Vanguard share snapshot is "
            f"{age_days} days old ({snapshot_date}); publishing price-drift "
            "weights rather than a fresh composition snapshot.",
            file=sys.stderr,
        )

    std = pd.DataFrame(
        {
            "raw_ticker": output["holding_ticker"].map(base.clean_text),
            "raw_name": output["holding_name"].map(base.clean_text),
            "weight": pd.to_numeric(output["weight"], errors="coerce"),
            "shares": pd.to_numeric(output["shares_held"], errors="coerce"),
            "market_value": pd.to_numeric(
                output["market_value_usd"],
                errors="coerce",
            ),
            "isin": "",
            "sedol": "",
        }
    )

    drifted = base.drift_vgt_with_yahoo(
        std,
        snapshot_date.isoformat(),
    )

    output["shares_held"] = drifted["shares"].values
    output["market_value_usd"] = drifted["market_value"].values
    output["weight"] = drifted["weight"].values
    output["retrieved_at_utc"] = (
        retrieved_at.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

    output = (
        output
        .sort_values("weight", ascending=False)
        .reset_index(drop=True)
    )
    output["rank"] = range(1, len(output) + 1)

    print(
        f"[VGT] Last-known Vanguard share snapshot repriced successfully: "
        f"{len(output)} rows, snapshot={snapshot_date}, age={age_days}d",
        file=sys.stderr,
    )

    return output[base.OUTPUT_COLS]

def fetch_invesco_fixed(
    session: Any,
    etf: str,
    timeout: int,
) -> base.FetchResult:
    """Official SOXQ pull with a browser-impersonated retry."""

    errors: list[str] = []

    result = _try_invesco_with_requests(
        session,
        etf,
        timeout,
        errors,
    )
    if result is not None:
        return result

    print(
        "[SOXQ] Standard Invesco requests blocked/failed; "
        "retrying with curl_cffi Chrome impersonation...",
        file=sys.stderr,
    )

    result = _try_invesco_with_curl_cffi(
        etf,
        timeout,
        errors,
    )
    if result is not None:
        return result

    detail = " | ".join(errors[-10:])
    raise base.HoldingsError(
        "SOXQ: all official Invesco routes failed after requests + "
        f"curl_cffi retry: {detail}"
    )


def _snapshot_date_from_output(frame: pd.DataFrame) -> date:
    values = (
        frame["source_date"]
        .dropna()
        .astype(str)
        .map(str.strip)
    )

    if values.empty:
        raise base.HoldingsError(
            "SOXQ: fallback has no source_date; cannot safely drift weights"
        )

    unique = list(dict.fromkeys(values.tolist()))
    if len(unique) != 1:
        raise base.HoldingsError(
            f"SOXQ: fallback contains multiple source dates: {unique[:5]}"
        )

    try:
        return datetime.fromisoformat(unique[0]).date()
    except ValueError as exc:
        raise base.HoldingsError(
            f"SOXQ: invalid fallback source_date {unique[0]!r}"
        ) from exc


def _reprice_soxq_snapshot(
    frame: pd.DataFrame,
    *,
    retrieved_at: datetime,
) -> pd.DataFrame:
    """Drift a stale official SOXQ share snapshot using current Yahoo prices."""

    import yfinance as yf

    output = frame.copy().reset_index(drop=True)
    snapshot_date = _snapshot_date_from_output(output)
    today = datetime.now(timezone.utc).date()
    age_days = (today - snapshot_date).days

    if age_days < 0:
        raise base.HoldingsError(
            f"SOXQ: source_date {snapshot_date} is in the future"
        )

    if age_days > SOXQ_MAX_DRIFT_DAYS:
        raise base.HoldingsError(
            "SOXQ: official composition snapshot is too old to drift safely: "
            f"{snapshot_date} ({age_days} days old; maximum "
            f"{SOXQ_MAX_DRIFT_DAYS})"
        )

    if age_days > SOXQ_WARN_DRIFT_DAYS:
        print(
            "[SOXQ] WARNING: official Invesco composition snapshot is "
            f"{age_days} days old ({snapshot_date}); publishing a price-drift "
            "estimate, not a fresh composition snapshot.",
            file=sys.stderr,
        )

    shares = pd.to_numeric(
        output["shares_held"],
        errors="coerce",
    )

    if shares.isna().any() or (shares <= 0).any():
        bad = output.loc[
            shares.isna() | (shares <= 0),
            ["holding_ticker", "holding_name", "shares_held"],
        ].head(10)
        raise base.HoldingsError(
            "SOXQ: fallback repricing requires positive shares for every "
            f"holding; bad rows={bad.to_dict(orient='records')}"
        )

    raw_tickers = output["holding_ticker"].map(base.clean_text)
    yahoo_symbols = raw_tickers.map(base.yahoo_ticker)

    if yahoo_symbols.eq("").any():
        bad = output.loc[
            yahoo_symbols.eq(""),
            ["holding_ticker", "holding_name"],
        ].head(10)
        raise base.HoldingsError(
            "SOXQ: fallback contains holdings without Yahoo-priceable tickers: "
            f"{bad.to_dict(orient='records')}"
        )

    symbols = sorted(set(yahoo_symbols.tolist()))
    start = (snapshot_date - timedelta(days=3)).isoformat()

    print(
        f"[SOXQ] Repricing all {len(output)} holdings from official "
        f"{snapshot_date} share snapshot via Yahoo...",
        file=sys.stderr,
    )

    try:
        history = yf.download(
            tickers=symbols,
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
        raise base.HoldingsError(
            f"SOXQ: Yahoo bulk repricing failed: {exc}"
        ) from exc

    if history is None or len(history) == 0:
        raise base.HoldingsError(
            "SOXQ: Yahoo returned no price history for fallback repricing"
        )

    closes = base._field_frame(history, "Close", symbols)
    splits = base._field_frame(history, "Stock Splits", symbols)

    latest_price: dict[str, float] = {}
    split_factor: dict[str, float] = {}

    for symbol in symbols:
        if symbol in closes.columns:
            series = pd.to_numeric(
                closes[symbol],
                errors="coerce",
            ).dropna()
            if not series.empty:
                price = float(series.iloc[-1])
                if math.isfinite(price) and price > 0:
                    latest_price[symbol] = price

        factor = 1.0
        if symbol in splits.columns:
            series = pd.to_numeric(
                splits[symbol],
                errors="coerce",
            ).fillna(0.0)
            for index, value in series.items():
                event_date = pd.Timestamp(index).date()
                if (
                    event_date > snapshot_date
                    and value not in (0, 1)
                    and value > 0
                ):
                    factor *= float(value)
        split_factor[symbol] = factor

    new_shares: list[float] = []
    new_market_values: list[float] = []
    missing_prices: list[str] = []

    for index in output.index:
        raw_ticker = raw_tickers.iloc[index]
        symbol = yahoo_symbols.iloc[index]
        share_count = float(shares.iloc[index])
        price = latest_price.get(symbol)

        if price is None:
            missing_prices.append(raw_ticker)
            new_shares.append(share_count)
            new_market_values.append(float("nan"))
            continue

        adjusted_shares = share_count * split_factor.get(symbol, 1.0)
        new_shares.append(adjusted_shares)
        new_market_values.append(adjusted_shares * price)

    if missing_prices:
        raise base.HoldingsError(
            "SOXQ: refusing to publish partially repriced fallback; Yahoo "
            "price missing for " + ", ".join(missing_prices[:15])
        )

    market_values = pd.Series(
        new_market_values,
        index=output.index,
        dtype=float,
    )

    total = float(market_values.sum())
    if not math.isfinite(total) or total <= 0:
        raise base.HoldingsError(
            f"SOXQ: invalid repriced market-value denominator {total}"
        )

    output["shares_held"] = new_shares
    output["market_value_usd"] = market_values
    output["weight"] = market_values / total * 100.0
    output["retrieved_at_utc"] = (
        retrieved_at.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

    # IMPORTANT: source_date is intentionally left unchanged. It represents the
    # official composition/share snapshot date, while retrieved_at_utc is the
    # date/time of the estimated weight drift.

    output = (
        output
        .sort_values("weight", ascending=False)
        .reset_index(drop=True)
    )
    output["rank"] = range(1, len(output) + 1)

    total_weight = float(pd.to_numeric(output["weight"]).sum())
    if not (99.999 <= total_weight <= 100.001):
        raise base.HoldingsError(
            f"SOXQ: repriced weights sum to {total_weight:.8f}%"
        )

    print(
        f"[SOXQ] Price-drift fallback OK: {len(output)} rows, "
        f"100.00000000%, snapshot={snapshot_date}, age={age_days}d",
        file=sys.stderr,
    )

    return output[base.OUTPUT_COLS]



def _guard_acwi_previous_snapshot(
    frame: pd.DataFrame,
) -> None:
    snapshot_date = _snapshot_date_from_output_for_label(frame, "ACWI")
    age_days = (
        datetime.now(timezone.utc).date()
        - snapshot_date
    ).days

    if age_days < 0:
        raise base.HoldingsError(
            f"ACWI: fallback source_date {snapshot_date} is in the future"
        )

    if age_days > ACWI_MAX_SOURCE_AGE_DAYS:
        raise base.HoldingsError(
            "ACWI: live iShares pull failed and last-known-good holdings are "
            f"too stale ({snapshot_date}, {age_days} days old)"
        )

    if age_days > ACWI_WARN_SOURCE_AGE_DAYS:
        print(
            "[ACWI] WARNING: using last-known-good iShares holdings from "
            f"{snapshot_date} ({age_days} days old).",
            file=sys.stderr,
        )

def fill_output_market_values_from_yahoo_fixed(
    frame: pd.DataFrame,
    *,
    label: str,
    retrieved_at: datetime,
    recompute_weights: bool = False,
) -> pd.DataFrame:
    """
    Preserve the original helper for normal backfills.

    When SOXQ requests weight recomputation, fully reprice the entire official
    share snapshot instead of only filling rows where old market_value_usd is
    null. That null-only condition was the bug that left SOXQ weights frozen.
    """

    normalized_label = label.upper()

    if normalized_label == "SOXQ" and recompute_weights:
        return _reprice_soxq_snapshot(
            frame,
            retrieved_at=retrieved_at,
        )

    if normalized_label == "VGT":
        return _reprice_vgt_snapshot(
            frame,
            retrieved_at=retrieved_at,
        )

    if normalized_label == "ACWI":
        _guard_acwi_previous_snapshot(frame)

    return _original_fill_output_market_values_from_yahoo(
        frame,
        label=label,
        retrieved_at=retrieved_at,
        recompute_weights=recompute_weights,
    )


# Monkeypatch the existing pipeline. Functions in base.run() resolve these names
# from the update_holdings module at runtime, so the rest of the code is reused.
base.fetch_vanguard = fetch_vanguard_fixed
base.fetch_ishares = fetch_ishares_fixed
base.fetch_invesco = fetch_invesco_fixed

base.FETCHERS["VGT"] = fetch_vanguard_fixed
base.FETCHERS["ACWI"] = fetch_ishares_fixed
base.FETCHERS["SOXQ"] = fetch_invesco_fixed

base.fill_output_market_values_from_yahoo = (
    fill_output_market_values_from_yahoo_fixed
)


if __name__ == "__main__":
    raise SystemExit(base.main())
