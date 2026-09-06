"""Modern Vanguard VGT PCF browser fallback.

The production updater's legacy direct PCF URL can now land on Vanguard's
interstitial / shell page without the server-rendered holdings table. This
module keeps that direct path as the first attempt, then establishes the
current Vanguard profile/PCF browser flow and scrapes the provider's DAILY PCF.

It deliberately does not fall back to month-end full holdings and does not
relax the existing VGT source-date freshness checks.
"""

from __future__ import annotations

import math
import re
import sys
import time
from typing import Any

import pandas as pd

import update_holdings as core


def _deep_text(driver: Any) -> str:
    """Visible text from the document plus all open shadow roots."""
    script = r"""
    const out = [];
    const roots = [document];
    const seen = new Set();
    while (roots.length) {
      const root = roots.shift();
      if (!root || seen.has(root)) continue;
      seen.add(root);
      try {
        const text = root === document
          ? (document.body ? document.body.innerText : '')
          : (root.host && root.host.shadowRoot ? root.host.shadowRoot.textContent : '');
        if (text) out.push(text);
      } catch (e) {}
      let elements = [];
      try { elements = root.querySelectorAll('*'); } catch (e) {}
      for (const el of elements) {
        if (el.shadowRoot) roots.push(el.shadowRoot);
      }
    }
    return out.join('\n');
    """
    try:
        return str(driver.execute_script(script) or "")
    except Exception:
        return ""


def _deep_click_contains(driver: Any, phrases: list[str]) -> bool:
    """Click the first visible clickable element across open shadow roots."""
    needles = [p.strip().lower() for p in phrases if p.strip()]
    script = r"""
    const needles = arguments[0];
    const roots = [document];
    const seen = new Set();
    const clickable = (el) => {
      const tag = (el.tagName || '').toLowerCase();
      const role = (el.getAttribute && (el.getAttribute('role') || '').toLowerCase()) || '';
      return ['button', 'a'].includes(tag) || ['button', 'tab', 'link'].includes(role);
    };
    const visible = (el) => {
      try {
        const style = window.getComputedStyle(el);
        return style && style.display !== 'none' && style.visibility !== 'hidden'
          && el.getClientRects().length > 0;
      } catch (e) { return true; }
    };
    while (roots.length) {
      const root = roots.shift();
      if (!root || seen.has(root)) continue;
      seen.add(root);
      let elements = [];
      try { elements = root.querySelectorAll('*'); } catch (e) {}
      for (const el of elements) {
        if (el.shadowRoot) roots.push(el.shadowRoot);
        if (!clickable(el) || !visible(el)) continue;
        if (el.disabled || (el.getAttribute && el.getAttribute('aria-disabled') === 'true')) continue;
        const text = ((el.innerText || el.textContent || '') + ' ' +
          ((el.getAttribute && el.getAttribute('aria-label')) || '') + ' ' +
          ((el.getAttribute && el.getAttribute('title')) || '')).trim().toLowerCase();
        if (needles.some(n => text.includes(n))) {
          try { el.scrollIntoView({block:'center'}); } catch (e) {}
          el.click();
          return true;
        }
      }
    }
    return false;
    """
    try:
        clicked = bool(driver.execute_script(script, needles))
    except Exception:
        clicked = False
    if clicked:
        time.sleep(0.8)
    return clicked


def _accept_vanguard_interstitial(driver: Any) -> None:
    """Best-effort acceptance of cookie / PCF terms interstitials."""
    core._vanguard_accept_dialog_if_present(driver)
    _deep_click_contains(
        driver,
        [
            "accept all",
            "accept cookies",
            "accept terms",
            "i accept",
            "agree and continue",
        ],
    )


def _wait_for_text(driver: Any, needle: str, timeout: int) -> None:
    from selenium.webdriver.support.ui import WebDriverWait

    target = needle.lower()
    WebDriverWait(driver, max(5, min(timeout, 60))).until(
        lambda d: target in _deep_text(d).lower()
    )


def _open_current_pcf(driver: Any, etf: str, timeout: int) -> None:
    """Navigate through Vanguard's current profile UI into the daily PCF view."""
    profile_url = (
        core.VANGUARD_PROFILE_URL.format(ticker=etf.lower())
        + "#portfolio-composition"
    )
    driver.get(profile_url)
    _accept_vanguard_interstitial(driver)

    _wait_for_text(driver, "Portfolio Composition File", max(timeout, 25))

    if not _deep_click_contains(driver, ["portfolio composition file"]):
        raise core.HoldingsError(
            "VGT: current Vanguard profile loaded but PCF tab could not be activated"
        )

    before_handles = set(driver.window_handles)
    view_phrases = [
        "view the pcf for this vanguard etf",
        "view portfolio composition files",
        "view the pcf",
    ]
    if not _deep_click_contains(driver, view_phrases):
        time.sleep(2.0)
        if not _deep_click_contains(driver, view_phrases):
            raise core.HoldingsError(
                "VGT: PCF tab opened but the provider PCF link was not found"
            )

    time.sleep(1.0)
    after_handles = set(driver.window_handles)
    new_handles = list(after_handles - before_handles)
    if new_handles:
        driver.switch_to.window(new_handles[-1])

    _accept_vanguard_interstitial(driver)
    time.sleep(1.0)
    _accept_vanguard_interstitial(driver)
    time.sleep(1.0)


def _deep_tables(driver: Any) -> list[dict[str, Any]]:
    """Return table headers/rows from the light DOM and all open shadow roots."""
    script = r"""
    const result = [];
    const roots = [document];
    const seen = new Set();
    while (roots.length) {
      const root = roots.shift();
      if (!root || seen.has(root)) continue;
      seen.add(root);
      let elements = [];
      try { elements = root.querySelectorAll('*'); } catch (e) {}
      for (const el of elements) {
        if (el.shadowRoot) roots.push(el.shadowRoot);
      }
      let tables = [];
      try { tables = root.querySelectorAll('table'); } catch (e) {}
      for (const table of tables) {
        let headers = Array.from(table.querySelectorAll('thead th'))
          .map(x => (x.innerText || x.textContent || '').trim());
        if (!headers.length) {
          const first = table.querySelector('tr');
          if (first) {
            headers = Array.from(first.querySelectorAll('th,td'))
              .map(x => (x.innerText || x.textContent || '').trim());
          }
        }
        let trs = Array.from(table.querySelectorAll('tbody tr'));
        if (!trs.length) trs = Array.from(table.querySelectorAll('tr')).slice(1);
        const rows = trs.map(tr =>
          Array.from(tr.querySelectorAll(':scope > td'))
            .map(x => (x.innerText || x.textContent || '').trim())
        ).filter(r => r.length);
        result.push({headers, rows});
      }
    }
    return result;
    """
    try:
        value = driver.execute_script(script)
        return value if isinstance(value, list) else []
    except Exception:
        return []


def _parse_deep_pcf_page(
    driver: Any,
) -> tuple[list[dict[str, Any]], str | None, tuple[int, int, int] | None]:
    """Parse one PCF page, including tables rendered inside open shadow roots."""
    tables = _deep_tables(driver)
    chosen: dict[str, Any] | None = None

    for table in tables:
        headers = [core.clean_text(x).lower() for x in table.get("headers", [])]
        has_ticker = any("ticker" in h or "symbol" in h for h in headers)
        has_shares = any("share" in h or "quantity" in h for h in headers)
        has_desc = any(
            "description" in h or "holding" in h or "security" in h or "name" in h
            for h in headers
        )
        if has_ticker and has_shares and has_desc:
            chosen = table
            break

    if chosen is None:
        header_samples = [
            " | ".join(core.clean_text(x) for x in table.get("headers", []))
            for table in tables[:8]
        ]
        raise core.HoldingsError(
            "VGT: current PCF view did not contain a Ticker/Description/Shares "
            f"table; table headers={header_samples}"
        )

    headers = [core.clean_text(x) for x in chosen.get("headers", [])]
    lower_headers = [h.lower() for h in headers]

    def index_for(*needles: str) -> int | None:
        for i, header in enumerate(lower_headers):
            if any(needle in header for needle in needles):
                return i
        return None

    ticker_i = index_for("ticker", "symbol")
    name_i = index_for("description", "holding", "security", "name")
    shares_i = index_for("shares", "share", "quantity")
    cusip_i = index_for("cusip")
    sedol_i = index_for("sedol")

    rows: list[dict[str, Any]] = []
    for values_raw in chosen.get("rows", []):
        values = [core.clean_text(v) for v in values_raw]

        def get(i: int | None) -> str:
            return values[i] if i is not None and i < len(values) else ""

        ticker = get(ticker_i)
        name = get(name_i)
        shares = core.to_float(get(shares_i))
        if not ticker or shares is None or shares <= 0:
            continue
        rows.append(
            {
                "raw_ticker": ticker,
                "raw_name": name or ticker,
                "weight": None,
                "shares": shares,
                "market_value": None,
                "isin": "",
                "sedol": get(sedol_i),
                "cusip": get(cusip_i),
            }
        )

    page_text = _deep_text(driver)
    page_date = core._vanguard_pcf_date(page_text)
    page_range = core._vanguard_pcf_page_range(page_text)
    return rows, page_date, page_range


def _deep_click_next(driver: Any) -> bool:
    script = r"""
    const roots = [document];
    const seen = new Set();
    const visible = (el) => {
      try {
        const style = window.getComputedStyle(el);
        return style && style.display !== 'none' && style.visibility !== 'hidden'
          && el.getClientRects().length > 0;
      } catch (e) { return true; }
    };
    while (roots.length) {
      const root = roots.shift();
      if (!root || seen.has(root)) continue;
      seen.add(root);
      let elements = [];
      try { elements = root.querySelectorAll('*'); } catch (e) {}
      for (const el of elements) {
        if (el.shadowRoot) roots.push(el.shadowRoot);
        const tag = (el.tagName || '').toLowerCase();
        const role = ((el.getAttribute && el.getAttribute('role')) || '').toLowerCase();
        if (!['button','a'].includes(tag) && !['button','link'].includes(role)) continue;
        if (!visible(el) || el.disabled || (el.getAttribute && el.getAttribute('aria-disabled') === 'true')) continue;
        const classes = ((el.getAttribute && el.getAttribute('class')) || '').toLowerCase();
        if (classes.includes('disabled')) continue;
        const label = (
          ((el.innerText || el.textContent || '') + ' ' +
           ((el.getAttribute && el.getAttribute('aria-label')) || '') + ' ' +
           ((el.getAttribute && el.getAttribute('title')) || ''))
        ).trim().toLowerCase();
        if (label === 'next' || label.includes(' next') || label.includes('next ') ||
            label === '›' || label === '⟩' || label === '>') {
          try { el.scrollIntoView({block:'center'}); } catch (e) {}
          el.click();
          return true;
        }
      }
    }
    return false;
    """
    try:
        clicked = bool(driver.execute_script(script))
    except Exception:
        clicked = False
    if clicked:
        time.sleep(0.5)
    return clicked


def _page_signature(driver: Any) -> tuple[str, str]:
    body = _deep_text(driver)
    range_match = re.findall(
        r"(?i)([\d,]+)\s*[-–—]\s*([\d,]+)\s+of\s+([\d,]+)",
        body,
    )
    page_range = "|".join(range_match[-1]) if range_match else ""
    return page_range, body[-1200:]


def _collect_rendered_pcf(
    driver: Any,
    etf: str,
    timeout: int,
) -> tuple[pd.DataFrame, str]:
    """Scrape all pages from the provider's rendered daily PCF table."""
    from selenium.webdriver.support.ui import WebDriverWait

    collected: dict[tuple[str, str], dict[str, Any]] = {}
    source_date: str | None = None
    expected_total: int | None = None
    seen_signatures: set[tuple[str, str]] = set()

    for _page_number in range(1, 80):
        WebDriverWait(driver, max(5, min(timeout, 30))).until(
            lambda d: len(_deep_tables(d)) > 0
        )

        rows, page_date, page_range = _parse_deep_pcf_page(driver)

        if page_date:
            if source_date and source_date != page_date:
                raise core.HoldingsError(
                    f"VGT: rendered PCF returned mixed dates {source_date} and {page_date}"
                )
            source_date = page_date

        if page_range:
            _first, _last, total = page_range
            expected_total = max(expected_total or 0, total)

        for row in rows:
            key = (
                core.canonical_ticker(row["raw_ticker"]),
                core.normalized_name(row["raw_name"]),
            )
            collected[key] = row

        if expected_total is not None and len(collected) >= expected_total:
            break

        signature = _page_signature(driver)
        if signature in seen_signatures:
            break
        seen_signatures.add(signature)

        old_signature = signature
        if not _deep_click_next(driver):
            break

        try:
            WebDriverWait(driver, max(5, min(timeout, 20))).until(
                lambda d: _page_signature(d) != old_signature
            )
        except Exception:
            time.sleep(0.75)
            if _page_signature(driver) == old_signature:
                break

    if not source_date:
        source_date = core._vanguard_pcf_date(_deep_text(driver))
    if not source_date:
        raise core.HoldingsError(
            "VGT: rendered Vanguard PCF loaded but its effective date was not found"
        )
    core.validate_vanguard_source_date(source_date)

    frame = pd.DataFrame(
        [
            {
                "raw_ticker": row["raw_ticker"],
                "raw_name": row["raw_name"],
                "weight": None,
                "shares": row["shares"],
                "market_value": None,
                "isin": row.get("isin", ""),
                "sedol": row.get("sedol", ""),
            }
            for row in collected.values()
        ],
        columns=core.STD_COLS,
    )
    frame = core.drop_nonsecurity_rows(frame)

    if len(frame) < core.MIN_HOLDINGS[etf]:
        raise core.HoldingsError(
            f"VGT: current-profile PCF collected only {len(frame)} security rows"
            + (
                f"; provider advertised {expected_total}"
                if expected_total is not None
                else ""
            )
        )

    if (
        expected_total is not None
        and len(frame) < max(core.MIN_HOLDINGS[etf], expected_total - 15)
    ):
        raise core.HoldingsError(
            f"VGT: current-profile PCF advertises {expected_total} rows "
            f"but only {len(frame)} securities were parsed"
        )

    return frame.reset_index(drop=True), source_date


def _fetch_vanguard_profile_pcf(etf: str, timeout: int) -> core.FetchResult:
    driver = None
    try:
        driver = core._make_chrome_driver()
        driver.set_page_load_timeout(max(45, timeout))
        _open_current_pcf(driver, etf, timeout)

        frame, source_date = _collect_rendered_pcf(driver, etf, timeout)
        valued = core._value_vanguard_daily_pcf(frame, source_date, etf)

        weights = pd.to_numeric(valued["weight"], errors="coerce")
        total = float(weights.sum(skipna=True))
        if int(weights.notna().sum()) < math.floor(len(valued) * 0.985):
            raise core.HoldingsError(
                "VGT: current-profile PCF produced too many blank weights"
            )
        if not (core.MIN_TOTAL_WEIGHT <= total <= core.MAX_TOTAL_WEIGHT):
            raise core.HoldingsError(
                f"VGT: current-profile PCF weight sum {total:.8f}% is invalid"
            )

        print(
            f"[VGT] Vanguard current-profile DAILY PCF: {len(valued)} priced "
            f"securities, source_date={source_date}",
            file=sys.stderr,
        )
        return core.FetchResult(
            frame=valued,
            source_date=source_date,
            precision_method="vanguard_profile_daily_pcf_x_prior_close",
        )
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def fetch_vanguard_with_profile_fallback(
    session: Any,
    etf: str,
    timeout: int,
) -> core.FetchResult:
    """Keep the legacy direct PCF path, then use Vanguard's current profile UI."""
    legacy_error: Exception | None = None
    try:
        return core.fetch_vanguard(session, etf, timeout)
    except Exception as exc:
        legacy_error = exc
        print(
            "[VGT] Legacy direct PCF path unavailable; "
            f"trying current profile PCF: {exc}",
            file=sys.stderr,
        )

    try:
        return _fetch_vanguard_profile_pcf(etf, timeout)
    except Exception as profile_exc:
        raise core.HoldingsError(
            "VGT: all DAILY Vanguard PCF paths failed: "
            f"legacy={type(legacy_error).__name__}: {legacy_error} | "
            f"profile={type(profile_exc).__name__}: {profile_exc}"
        ) from profile_exc
