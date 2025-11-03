#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Custom Prometheus exporter for:
- Weather (Open-Meteo)
- FX rates USD/EUR -> KZT (Frankfurter + exchangerate.host fallback)
- GitHub repo stats (stars, forks, open issues)

Metrics are exposed on /metrics (default port 8000).
Update interval defaults to 20 seconds.

Environment overrides (optional):
  PORT=8000
  LOOP_SECONDS=20
  CITY_LAT=51.1694
  CITY_LON=71.4491
  GITHUB_REPO=torvalds/linux        (format: owner/repo)
"""

import os
import time
import logging
from typing import Tuple

import requests
from prometheus_client import Gauge, start_http_server

# -----------------------------
# Configuration (env overrides)
# -----------------------------
PORT = int(os.getenv("PORT", "8000"))
LOOP_SECONDS = int(os.getenv("LOOP_SECONDS", "20"))

CITY_LAT = float(os.getenv("CITY_LAT", "51.1694"))   # Astana default
CITY_LON = float(os.getenv("CITY_LON", "71.4491"))

GITHUB_REPO = os.getenv("GITHUB_REPO", "torvalds/linux")  # owner/name

HTTP_TIMEOUT = 8  # seconds for outbound API calls

# ------------
# Logging
# ------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("custom_exporter")


# -------------------------
# Prometheus metric objects
# -------------------------
# Weather
g_temp_c = Gauge("openmeteo_temperature_c", "Air temperature (C)")
g_windspeed_ms = Gauge("openmeteo_windspeed_ms", "Wind speed (m/s)")
g_winddir_deg = Gauge("openmeteo_winddir_deg", "Wind direction (degrees)")
g_openmeteo_up = Gauge("openmeteo_api_up", "Open-Meteo reachable (1/0)")

# FX
g_fx_usd_kzt = Gauge("fx_usd_kzt", "USD->KZT rate")
g_fx_eur_kzt = Gauge("fx_eur_kzt", "EUR->KZT rate")
g_fx_up = Gauge("fx_api_up", "FX API reachable (1/0)")

# GitHub
g_gh_stars = Gauge("github_repo_stars", "GitHub repo stars")
g_gh_forks = Gauge("github_repo_forks", "GitHub repo forks")
g_gh_open_issues = Gauge("github_repo_open_issues", "GitHub repo open issues")
g_gh_up = Gauge("gh_api_up", "GitHub API reachable (1/0)")


# ----------------
# Helper fetchers
# ----------------
def fetch_weather(lat: float, lon: float) -> Tuple[float, float, float, bool]:
    """
    Returns (temp_c, windspeed_ms, winddir_deg, ok).
    """
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,wind_speed_10m,wind_direction_10m",
            },
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        j = r.json()
        cur = j.get("current", {}) or {}
        temp_c = float(cur.get("temperature_2m"))
        windspeed = float(cur.get("wind_speed_10m"))
        winddir = float(cur.get("wind_direction_10m"))
        return temp_c, windspeed, winddir, True
    except Exception as e:
        log.warning("[WEATHER] fetch failed: %s", e)
        return 0.0, 0.0, 0.0, False


def fetch_fx_rates():
    """
    Returns (usd_kzt, eur_kzt, ok).
    Primary: open.er-api.com (KZT supported)
    Fallback: jsdelivr currency-api (daily JSON on GitHub)
    Last resort: exchangerate.host (some networks return partial JSON)
    """
    import requests

    # --- Primary: ERAPI (usually reliable) ---
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        r.raise_for_status()
        usd_json = r.json()
        usd_kzt = float(usd_json["rates"]["KZT"])

        r2 = requests.get("https://open.er-api.com/v6/latest/EUR", timeout=8)
        r2.raise_for_status()
        eur_json = r2.json()
        eur_kzt = float(eur_json["rates"]["KZT"])
        return usd_kzt, eur_kzt, True
    except Exception as e:
        log.warning("[FX] ERAPI failed: %s", e)

    # --- Fallback: jsdelivr (GitHub daily currency API) ---
    try:
        r = requests.get("https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies/usd/kzt.json", timeout=8)
        r.raise_for_status()
        usd_kzt = float(r.json()["kzt"])

        r2 = requests.get("https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies/eur/kzt.json", timeout=8)
        r2.raise_for_status()
        eur_kzt = float(r2.json()["kzt"])
        return usd_kzt, eur_kzt, True
    except Exception as e:
        log.warning("[FX] jsDelivr currency-api failed: %s", e)

    # --- Last resort: exchangerate.host (works in many envs) ---
    try:
        r = requests.get("https://api.exchangerate.host/latest", params={"base": "USD", "symbols": "KZT"}, timeout=8)
        r.raise_for_status()
        usd_kzt = float(r.json().get("rates", {}).get("KZT"))

        r2 = requests.get("https://api.exchangerate.host/latest", params={"base": "EUR", "symbols": "KZT"}, timeout=8)
        r2.raise_for_status()
        eur_kzt = float(r2.json().get("rates", {}).get("KZT"))

        if usd_kzt and eur_kzt:
            return usd_kzt, eur_kzt, True
        raise ValueError("Missing rates in exchangerate.host response")
    except Exception as e:
        log.warning("[FX] exchangerate.host failed: %s", e)

    # if all fail
    return 0.0, 0.0, False



def fetch_github(repo: str) -> Tuple[int, int, int, bool]:
    """
    Returns (stars, forks, open_issues, ok) for a repo like 'owner/name'.
    """
    try:
        url = f"https://api.github.com/repos/{repo}"
        r = requests.get(
            url,
            headers={"User-Agent": "custom-exporter"},
            timeout=HTTP_TIMEOUT,
        )
        # GitHub may rate-limit with 403; still parse message
        r.raise_for_status()
        j = r.json()
        stars = int(j.get("stargazers_count", 0))
        forks = int(j.get("forks_count", 0))
        open_issues = int(j.get("open_issues_count", 0))
        return stars, forks, open_issues, True
    except Exception as e:
        log.warning("[GITHUB] fetch failed for %s: %s", repo, e)
        return 0, 0, 0, False


# -------------
# Main loop
# -------------
def main() -> None:
    log.info(
        "Starting custom exporter on port %d, loop=%ds, city=(%s,%s), repo=%s",
        PORT, LOOP_SECONDS, CITY_LAT, CITY_LON, GITHUB_REPO,
    )
    start_http_server(PORT)

    while True:
        # WEATHER
        temp_c, wind_ms, wind_deg, ok_w = fetch_weather(CITY_LAT, CITY_LON)
        g_temp_c.set(temp_c)
        g_windspeed_ms.set(wind_ms)
        g_winddir_deg.set(wind_deg)
        g_openmeteo_up.set(1 if ok_w else 0)

        # FX
        usd_kzt, eur_kzt, ok_fx = fetch_fx_rates()
        g_fx_usd_kzt.set(usd_kzt)
        g_fx_eur_kzt.set(eur_kzt)
        g_fx_up.set(1 if ok_fx else 0)

        # GITHUB
        stars, forks, issues, ok_gh = fetch_github(GITHUB_REPO)
        g_gh_stars.set(stars)
        g_gh_forks.set(forks)
        g_gh_open_issues.set(issues)
        g_gh_up.set(1 if ok_gh else 0)

        log.info(
            "OK(w=%s fx=%s gh=%s)  temp=%.1fC wind=%.1fm/s usd_kzt=%.3f eur_kzt=%.3f stars=%d forks=%d issues=%d",
            ok_w, ok_fx, ok_gh, temp_c, wind_ms, usd_kzt, eur_kzt, stars, forks, issues
        )

        time.sleep(LOOP_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Exporter stopped by user.")
