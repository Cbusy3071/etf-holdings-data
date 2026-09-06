"""Production entry point with Vanguard current-profile PCF fallback enabled."""

import update_holdings as pipeline
from vgt_profile_pcf import fetch_vanguard_with_profile_fallback

pipeline.FETCHERS["VGT"] = fetch_vanguard_with_profile_fallback

if __name__ == "__main__":
    raise SystemExit(pipeline.main())
