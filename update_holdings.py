from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path


OUTPUT_FILE = Path("data/ETF_Holdings_Latest.csv")


def collect_holdings() -> list[dict[str, object]]:
    """
    Temporary test data.

    Replace this function later with the actual API, issuer-file
    download or scraping process.
    """
    update_time = datetime.now(timezone.utc).isoformat()

    return [
        {
            "fund_ticker": "TEST",
            "holding_ticker": "ABC",
            "holding_name": "Test Holding",
            "weight": 1.0,
            "source_date": update_time,
        }
    ]


def main() -> None:
    rows = collect_holdings()

    if not rows:
        raise RuntimeError(
            "No holdings were collected. Existing output was not replaced."
        )

    required_columns = [
        "fund_ticker",
        "holding_ticker",
        "holding_name",
        "weight",
        "source_date",
    ]

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    temporary_file = OUTPUT_FILE.with_suffix(".tmp")

    with temporary_file.open(
        mode="w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.DictWriter(file, fieldnames=required_columns)
        writer.writeheader()
        writer.writerows(rows)

    # Replace the live CSV only after the new file has been written.
    temporary_file.replace(OUTPUT_FILE)

    print(f"Wrote {len(rows)} records to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()