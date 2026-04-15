#!/usr/bin/env python3
"""
Meta Data Snapshot — Pulls 14 days of campaign/adset/ad insights plus
active entity metadata and creative thumbnails, writes to data/snapshots/
for the dashboard to consume.

Runs 3x/day via GitHub Actions (8am/12pm/4pm MYT). All output is written
atomically at the very end — if any fetch fails, existing snapshots are
left intact.

Env vars required:
  META_TOKEN  — Meta Graph API access token
"""

import os
import sys
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

# --- Config ---
API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"
ACCOUNT_ID = "1000723654649396"
OUT_DIR = os.environ.get("OUT_DIR", "data/snapshots")
SCHEMA_VERSION = 1

# Window sizes — match Meta's silent-cap limits for ad-level daily granularity.
CAMPAIGN_DAYS = 14
ADSET_DAYS = 14
AD_DAYS = 14

META_TOKEN = os.environ.get("META_TOKEN", "")
if not META_TOKEN:
    print("ERROR: META_TOKEN env var not set", file=sys.stderr)
    sys.exit(1)


def _log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def _get_json(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "im8-snapshot"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        # Surface Meta's actual error message instead of a bare 400/500
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        # Strip access_token from URL for log safety
        safe_url = url.split("access_token=")[0] + "access_token=REDACTED&" + url.split("&", 1)[-1] if "access_token=" in url else url
        raise RuntimeError(f"HTTP {e.code} from Meta — body: {body[:500]} — url: {safe_url[:200]}") from e


def _paginate(initial_url, timeout=120):
    """Walk Meta's paging.next links until exhausted."""
    out = []
    url = initial_url
    while url:
        data = _get_json(url, timeout=timeout)
        out.extend(data.get("data") or [])
        url = (data.get("paging") or {}).get("next")
    return out


# --- Insights fetch ---
def fetch_insights(level, date_from, date_to):
    """
    level: 'campaign' | 'adset' | 'ad'
    Returns list of raw Meta insight rows.
    """
    fields_map = {
        "campaign": "campaign_id,campaign_name,spend,actions,action_values",
        "adset": "adset_id,adset_name,campaign_id,campaign_name,spend,actions,action_values",
        "ad": "ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,spend,actions,action_values",
    }
    params = {
        "access_token": META_TOKEN,
        "level": level,
        "fields": fields_map[level],
        "time_range": json.dumps({"since": date_from, "until": date_to}),
        "time_increment": "1",
        "limit": "500",
    }
    url = f"{BASE_URL}/act_{ACCOUNT_ID}/insights?{urllib.parse.urlencode(params)}"
    return _paginate(url)


# --- Active entity fetch (with metadata) ---
def fetch_active_entities(endpoint, extra_fields):
    """
    endpoint: 'campaigns' | 'adsets' | 'ads'
    extra_fields: comma-separated extra fields to pull
    Returns list of {id, name, ...extra}.
    """
    params = {
        "access_token": META_TOKEN,
        "fields": "id,name," + extra_fields,
        "filtering": json.dumps(
            [{"field": "effective_status", "operator": "IN", "value": ["ACTIVE"]}]
        ),
        "limit": "500",
    }
    url = f"{BASE_URL}/act_{ACCOUNT_ID}/{endpoint}?{urllib.parse.urlencode(params)}"
    return _paginate(url)


# --- Creatives (thumbnails) for a list of ad IDs ---
def fetch_creatives(ad_ids):
    """
    Batch creatives in groups of 50. Returns dict of adId -> {thumb, hiRes, preview}.
    """
    out = {}
    for i in range(0, len(ad_ids), 50):
        batch = ad_ids[i : i + 50]
        url = (
            f"{BASE_URL}/?ids={','.join(batch)}"
            "&fields=creative{thumbnail_url,image_url,effective_object_story_id}"
            f"&access_token={META_TOKEN}"
        )
        try:
            data = _get_json(url)
            for ad_id, d in data.items():
                c = d.get("creative") or {}
                thumb = c.get("thumbnail_url") or c.get("image_url")
                hi_res = c.get("image_url") or c.get("thumbnail_url")
                story_id = c.get("effective_object_story_id")
                out[ad_id] = {
                    "thumb": thumb,
                    "hiRes": hi_res,
                    "preview": f"https://www.facebook.com/{story_id}" if story_id else None,
                }
        except Exception as e:
            _log(f"creative batch {i} failed: {type(e).__name__}: {e}")
    return out


# --- Date helpers ---
def fmt_date(d):
    return d.strftime("%Y-%m-%d")


def compute_dates():
    """Use NY timezone so 'today' matches how the Meta dashboard thinks."""
    now_utc = datetime.now(timezone.utc)
    year = now_utc.year
    # DST window (approx)
    mar1 = datetime(year, 3, 1, tzinfo=timezone.utc)
    days_to_sunday = (6 - mar1.weekday()) % 7
    dst_start = mar1.replace(day=1 + days_to_sunday + 7, hour=2)
    nov1 = datetime(year, 11, 1, tzinfo=timezone.utc)
    days_to_sunday = (6 - nov1.weekday()) % 7
    dst_end = nov1.replace(day=1 + days_to_sunday if days_to_sunday > 0 else 1, hour=2)
    ny_offset = -4 if dst_start <= now_utc < dst_end else -5
    now_ny = now_utc + timedelta(hours=ny_offset)
    today = now_ny.date()
    return today


# --- Main ---
def main():
    start = time.time()
    today = compute_dates()
    _log(f"Snapshot run: today (NY)={today}")

    campaign_from = fmt_date(today - timedelta(days=CAMPAIGN_DAYS - 1))
    adset_from = fmt_date(today - timedelta(days=ADSET_DAYS - 1))
    ad_from = fmt_date(today - timedelta(days=AD_DAYS - 1))
    date_to = fmt_date(today)

    # Collect all data in memory first. Only write files at the end.
    data_out = {}

    try:
        # Fire independent fetches in parallel
        with ThreadPoolExecutor(max_workers=6) as pool:
            f_camp_insights = pool.submit(fetch_insights, "campaign", campaign_from, date_to)
            f_adset_insights = pool.submit(fetch_insights, "adset", adset_from, date_to)
            f_ad_insights = pool.submit(fetch_insights, "ad", ad_from, date_to)
            f_active_camps = pool.submit(
                fetch_active_entities,
                "campaigns",
                "daily_budget,bid_strategy,status,effective_status",
            )
            f_active_adsets = pool.submit(
                fetch_active_entities, "adsets", "daily_budget,campaign_id,effective_status"
            )
            f_active_ads = pool.submit(fetch_active_entities, "ads", "effective_status")

            data_out["campaign_insights"] = f_camp_insights.result()
            _log(f"  campaign insights: {len(data_out['campaign_insights'])} rows ({time.time()-start:.1f}s)")
            data_out["adset_insights"] = f_adset_insights.result()
            _log(f"  adset insights:    {len(data_out['adset_insights'])} rows ({time.time()-start:.1f}s)")
            data_out["ad_insights"] = f_ad_insights.result()
            _log(f"  ad insights:       {len(data_out['ad_insights'])} rows ({time.time()-start:.1f}s)")
            data_out["active_campaigns"] = f_active_camps.result()
            _log(f"  active campaigns:  {len(data_out['active_campaigns'])}")
            data_out["active_adsets"] = f_active_adsets.result()
            _log(f"  active adsets:     {len(data_out['active_adsets'])}")
            data_out["active_ads"] = f_active_ads.result()
            _log(f"  active ads:        {len(data_out['active_ads'])}")

        # Fetch creatives only for active ads (keeps payload manageable)
        active_ad_ids = [a["id"] for a in data_out["active_ads"] if a.get("id")]
        _log(f"Fetching creatives for {len(active_ad_ids)} active ads...")
        data_out["creatives"] = fetch_creatives(active_ad_ids)
        _log(f"  creatives: {len(data_out['creatives'])} ({time.time()-start:.1f}s)")

    except Exception as e:
        _log(f"FATAL: fetch aborted ({type(e).__name__}): {e}")
        _log("Existing snapshot files left unchanged.")
        sys.exit(1)

    # All fetches succeeded — now write files atomically
    _log(f"All fetches complete in {time.time()-start:.1f}s, writing files to {OUT_DIR}/")
    os.makedirs(OUT_DIR, exist_ok=True)

    def write_file(name, payload):
        path = os.path.join(OUT_DIR, name)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        os.replace(tmp, path)
        _log(f"  wrote {name} ({os.path.getsize(path):,} bytes)")

    write_file("campaigns-insights.json", data_out["campaign_insights"])
    write_file("adsets-insights.json", data_out["adset_insights"])
    write_file("ads-insights.json", data_out["ad_insights"])
    write_file("campaigns-meta.json", data_out["active_campaigns"])
    write_file("adsets-meta.json", data_out["active_adsets"])
    write_file(
        "active-ids.json",
        {
            "campaigns": [c["id"] for c in data_out["active_campaigns"] if c.get("id")],
            "adsets": [a["id"] for a in data_out["active_adsets"] if a.get("id")],
            "ads": [a["id"] for a in data_out["active_ads"] if a.get("id")],
        },
    )
    write_file("creatives.json", data_out["creatives"])
    write_file(
        "metadata.json",
        {
            "schema_version": SCHEMA_VERSION,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "account_id": ACCOUNT_ID,
            "windows": {
                "campaigns_days": CAMPAIGN_DAYS,
                "adsets_days": ADSET_DAYS,
                "ads_days": AD_DAYS,
            },
            "counts": {
                "campaign_rows": len(data_out["campaign_insights"]),
                "adset_rows": len(data_out["adset_insights"]),
                "ad_rows": len(data_out["ad_insights"]),
                "active_campaigns": len(data_out["active_campaigns"]),
                "active_adsets": len(data_out["active_adsets"]),
                "active_ads": len(data_out["active_ads"]),
                "creatives": len(data_out["creatives"]),
            },
        },
    )

    _log(f"Snapshot complete in {time.time()-start:.1f}s total")


if __name__ == "__main__":
    main()
