name: Update ETF Holdings

on:
  workflow_dispatch:

  schedule:
    # Early attempt. If provider files are not fresh yet, the freshness gate
    # below fails the run and nothing is published.
    - cron: "7 7 * * 2-6"
      timezone: "Australia/Sydney"

    # Later retry after providers have had more time to publish daily holdings.
    - cron: "17 18 * * 2-6"
      timezone: "Australia/Sydney"

permissions:
  contents: write

concurrency:
  group: etf-holdings-update
  cancel-in-progress: false

jobs:
  update-holdings:
    runs-on: ubuntu-latest

    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.13"

      - name: Set up Chrome for SOXQ holdings
        id: chrome
        uses: browser-actions/setup-chrome@v2
        with:
          install-chromedriver: true

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas requests yfinance openpyxl lxml html5lib curl_cffi selenium

      - name: Check Python syntax
        run: python -m py_compile update_holdings.py

      - name: Create output directory
        run: mkdir -p data

      - name: Download ETF holdings
        env:
          CHROME_BIN: ${{ steps.chrome.outputs['chrome-path'] }}
          CHROMEDRIVER: ${{ steps.chrome.outputs['chromedriver-path'] }}
        run: |
          python update_holdings.py \
            --out-dir data \
            --combined-name ETF_Holdings_Latest.csv \
            VGT ACWI XLF XLI XLC PPH MLPX GRID SOXQ

      - name: Validate generated output
        run: |
          test -s data/ETF_Holdings_Latest.csv

          python - <<'PY'
          import pandas as pd

          path = "data/ETF_Holdings_Latest.csv"
          df = pd.read_csv(path)

          expected_columns = [
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

          expected_funds = {
              "VGT", "ACWI", "XLF", "XLI", "XLC",
              "PPH", "MLPX", "GRID", "SOXQ",
          }

          if list(df.columns) != expected_columns:
              raise SystemExit(
                  f"Unexpected CSV columns: {list(df.columns)}"
              )

          actual_funds = set(
              df["fund_ticker"].dropna().astype(str).unique()
          )

          if actual_funds != expected_funds:
              raise SystemExit(
                  f"ETF mismatch. Expected {expected_funds}, got {actual_funds}"
              )

          source_dates = {}

          for ticker, group in df.groupby("fund_ticker"):
              parsed = pd.to_datetime(
                  group["source_date"],
                  errors="coerce",
              ).dropna()

              if parsed.empty:
                  raise SystemExit(
                      f"{ticker}: no valid source_date"
                  )

              dates = sorted(
                  {
                      value.date()
                      for value in parsed
                  }
              )

              if len(dates) != 1:
                  raise SystemExit(
                      f"{ticker}: mixed source dates {dates}"
                  )

              source_dates[str(ticker)] = dates[0]

          # XLF, XLI and XLC are used as the daily freshness anchor.
          anchor_funds = ("XLF", "XLI", "XLC")

          anchor_dates = {
              ticker: source_dates[ticker]
              for ticker in anchor_funds
          }

          if len(set(anchor_dates.values())) != 1:
              raise SystemExit(
                  "State Street anchor funds disagree: "
                  + ", ".join(
                      f"{ticker}={value.isoformat()}"
                      for ticker, value in anchor_dates.items()
                  )
              )

          anchor_date = next(iter(anchor_dates.values()))
          today_utc = pd.Timestamp.now(tz="UTC").date()

          def weekday_steps(start, end):
              count = 0
              cursor = start

              while cursor < end:
                  cursor = (
                      pd.Timestamp(cursor)
                      + pd.Timedelta(days=1)
                  ).date()

                  if cursor.weekday() < 5:
                      count += 1

              return count

          # Prevent the anchor itself becoming badly stale.
          # Allows weekends and roughly one market holiday.
          if weekday_steps(anchor_date, today_utc) > 2:
              raise SystemExit(
                  "State Street anchor is stale: "
                  f"{anchor_date.isoformat()} vs {today_utc.isoformat()}"
              )

          # Maximum allowed lag compared with XLF/XLI/XLC.
          max_lag_days = {
              "VGT": 1,
              "ACWI": 2,
              "XLF": 0,
              "XLI": 0,
              "XLC": 0,
              "PPH": 2,
              "MLPX": 1,
              "GRID": 1,
              "SOXQ": 1,
          }

          stale = []

          for ticker, allowed_lag in max_lag_days.items():
              lag = (
                  anchor_date
                  - source_dates[ticker]
              ).days

              if lag < 0:
                  stale.append(
                      f"{ticker}={source_dates[ticker]} is ahead of "
                      f"anchor={anchor_date}"
                  )

              elif lag > allowed_lag:
                  stale.append(
                      f"{ticker}={source_dates[ticker]} lags "
                      f"anchor={anchor_date} by {lag}d "
                      f"(limit {allowed_lag}d)"
                  )

          if stale:
              raise SystemExit(
                  "Refusing to publish stale ETF composition:\n  - "
                  + "\n  - ".join(stale)
              )

          print(
              f"Validated {len(df):,} rows "
              f"across {len(actual_funds)} ETFs"
          )

          for ticker, group in df.groupby("fund_ticker"):
              print(
                  ticker,
                  len(group),
                  f"weight={group['weight'].sum():.8f}%",
                  f"source_date={group['source_date'].iloc[0]}",
              )
          PY

      - name: Commit updated CSV
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

          cp data/ETF_Holdings_Latest.csv /tmp/ETF_Holdings_Latest.csv

          git fetch origin main

          if [ "$(git rev-parse origin/main)" != "$GITHUB_SHA" ]; then
            echo "Main changed while this workflow was running."
            echo "Refusing to publish output generated from stale code."
            exit 1
          fi

          git reset --hard origin/main

          # Ignore retrieved_at_utc when checking whether holdings actually
          # changed. This stops the later retry making duplicate commits.
          if [ -f data/ETF_Holdings_Latest.csv ]; then
            SAME_HOLDINGS="$(python - <<'PY'
          import pandas as pd

          old = pd.read_csv("data/ETF_Holdings_Latest.csv")
          new = pd.read_csv("/tmp/ETF_Holdings_Latest.csv")

          compare_columns = [
              column
              for column in new.columns
              if column != "retrieved_at_utc"
          ]

          same = (
              list(old.columns) == list(new.columns)
              and len(old) == len(new)
              and old[compare_columns].equals(new[compare_columns])
          )

          print("yes" if same else "no")
          PY
          )"

            if [ "$SAME_HOLDINGS" = "yes" ]; then
              echo "No substantive holdings changes detected."
              exit 0
            fi
          fi

          mkdir -p data
          cp /tmp/ETF_Holdings_Latest.csv data/ETF_Holdings_Latest.csv

          git add data/ETF_Holdings_Latest.csv

          if git diff --cached --quiet; then
            echo "No holdings changes detected."
            exit 0
          fi

          git commit -m "Update ETF holdings $(date -u +'%Y-%m-%d %H:%M UTC')"
          git push origin HEAD:main

          echo "Holdings CSV successfully published."
