"""Production entry point.

Temporary VGT source: Perplexity Finance holdings endpoint.
The other ETFs continue to use the provider-specific fetchers in update_holdings.py.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

import update_holdings as pipeline

PERPLEXITY_VGT_URL = "https://www.perplexity.ai/rest/finance/holdings/{ticker}"


def _extract_records(payload: Any, ticker: str) -> list[dict[str, Any]]:
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
            raise pipeline.HoldingsError(
                f"{ticker}: unexpected Perplexity JSON structure; "
                f"keys={sorted(payload.keys())[:12]}"
            )
    else:
        raise pipeline.HoldingsError(
            f"{ticker}: unexpected Perplexity JSON type {type(payload).__name__}"
        )

    if not records or not all(isinstance(item, dict) for item in records):
        raise pipeline.HoldingsError(f"{ticker}: malformed/empty Perplexity holdings")
    return records


def fetch_vgt_perplexity(session: Any, etf: str, timeout: int) -> pipeline.FetchResult:
    ticker = etf.strip().upper()
    response = session.get(
        PERPLEXITY_VGT_URL.format(ticker=ticker),
        headers={
            **pipeline.HEADERS,
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.perplexity.ai/finance/{ticker}",
        },
        timeout=(10, timeout),
        allow_redirects=True,
    )

    if response.status_code != 200:
        preview = str(response.text or "")[:220].replace("\n", " ")
        raise pipeline.HoldingsError(
            f"{ticker}: Perplexity HTTP {response.status_code}; body={preview!r}"
        )

    try:
        payload = response.json()
    except Exception as exc:
        preview = str(response.text or "")[:220].replace("\n", " ")
        raise pipeline.HoldingsError(
            f"{ticker}: Perplexity returned non-JSON content: {preview!r}"
        ) from exc

    records = _extract_records(payload, ticker)
    rows: list[dict[str, Any]] = []

    for entity in records:
        raw_ticker = pipeline.clean_text(
            entity.get("symbol") or entity.get("ticker") or ""
        )
        raw_name = pipeline.clean_text(
            entity.get("name")
            or entity.get("holding_name")
            or entity.get("securityName")
            or ""
        )
        weight = pipeline.to_float(
            entity.get("weight")
            if entity.get("weight") is not None
            else entity.get("percentWeight")
        )
        shares = pipeline.to_float(
            entity.get("shares")
            if entity.get("shares") is not None
            else entity.get("sharesHeld")
        )
        market_value = pipeline.to_float(
            entity.get("market_value")
            if entity.get("market_value") is not None
            else entity.get("marketValue")
        )

        if not raw_ticker or weight is None:
            continue

        rows.append(
            {
                "raw_ticker": raw_ticker,
                "raw_name": raw_name or raw_ticker,
                "weight": weight,
                "shares": shares,
                "market_value": market_value,
                "isin": pipeline.clean_text(entity.get("isin") or ""),
                "sedol": pipeline.clean_text(entity.get("sedol") or ""),
            }
        )

    frame = pd.DataFrame(rows, columns=pipeline.STD_COLS)
    frame = pipeline.drop_nonsecurity_rows(frame)

    if len(frame) < pipeline.MIN_HOLDINGS[ticker]:
        raise pipeline.HoldingsError(
            f"{ticker}: Perplexity returned only {len(frame)} security rows; "
            f"required at least {pipeline.MIN_HOLDINGS[ticker]}"
        )

    weights = pd.to_numeric(frame["weight"], errors="coerce")
    if weights.isna().any():
        raise pipeline.HoldingsError(
            f"{ticker}: Perplexity returned {int(weights.isna().sum())} blank weights"
        )

    total = float(weights.sum())

    # Normalise weight units first. Perplexity has historically returned
    # percentage points (e.g. 16.10), but tolerate fractional weights too.
    if 0.97 <= total <= 1.03:
        weights = weights * 100.0
        frame["weight"] = weights
        total = float(weights.sum())

    # Perplexity may display/report `weight` at only ~2 decimals while exposing
    # much more precise market values. Do not manufacture extra digits by
    # formatting a rounded percentage. Instead infer the portfolio denominator
    # from market_value / reported_weight across substantial holdings, using the
    # median for robustness, then recompute each available weight from its market
    # value. This preserves a cash/non-security gap better than forcing the
    # security market values themselves to sum to exactly 100%.
    market_values = pd.to_numeric(frame["market_value"], errors="coerce")
    usable = (
        market_values.notna()
        & (market_values > 0)
        & weights.notna()
        & (weights >= 0.05)
    )

    if int(usable.sum()) >= 20:
        implied_denominators = (
            market_values.loc[usable]
            / (weights.loc[usable] / 100.0)
        )
        denominator = float(implied_denominators.median())

        # Reject an unstable denominator estimate rather than pretending it is
        # precise. A 5% median-relative dispersion limit is deliberately loose
        # enough for 2-decimal reported weights but catches inconsistent fields.
        relative_errors = (
            (implied_denominators - denominator).abs() / denominator
        )
        median_relative_error = float(relative_errors.median())

        if denominator > 0 and median_relative_error <= 0.05:
            precise = market_values / denominator * 100.0
            frame.loc[market_values.notna(), "weight"] = precise.loc[
                market_values.notna()
            ]
            weights = pd.to_numeric(frame["weight"], errors="coerce")
            total = float(weights.sum())
            method = "perplexity_market_value_denominator_recomputed"
        else:
            method = "perplexity_provider_weight"
    else:
        method = "perplexity_provider_weight"

    if not (pipeline.MIN_TOTAL_WEIGHT <= total <= pipeline.MAX_TOTAL_WEIGHT):
        raise pipeline.HoldingsError(
            f"{ticker}: Perplexity weight sum {total:.8f}% outside "
            f"{pipeline.MIN_TOTAL_WEIGHT:.0f}-{pipeline.MAX_TOTAL_WEIGHT:.0f}%"
        )

    frame = frame.sort_values("weight", ascending=False).reset_index(drop=True)

    return pipeline.FetchResult(
        frame=frame[pipeline.STD_COLS],
        source_date=pipeline.iso_today(),
        precision_method=f"{method}_retrieval_date",
    )


pipeline.SCRIPT_REVISION = "2026-09-06-vgt-perplexity-high-precision-v2"
pipeline.FETCHERS["VGT"] = fetch_vgt_perplexity

if __name__ == "__main__":
    raise SystemExit(pipeline.main())
