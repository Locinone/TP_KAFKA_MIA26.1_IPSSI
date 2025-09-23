import os
import time
from typing import List, Dict, Any
import requests
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

WEBHDFS_BASE = os.getenv("WEBHDFS_BASE", "http://namenode:9870/webhdfs/v1")
WEATHER_DATA_PATH = os.getenv("WEATHER_DATA_PATH", "/weather-data")
ALERTS_PATH = os.getenv("ALERTS_PATH", "/hdfs-data")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "5"))

app = FastAPI(title="Weather Analytics API")


def webhdfs_list(path: str) -> Dict[str, Any]:
    url = f"{WEBHDFS_BASE}{path}?op=LISTSTATUS"
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def webhdfs_open(path: str) -> str:
    url = f"{WEBHDFS_BASE}{path}?op=OPEN"
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text


def list_recursive_files(base_path: str, prefix_filters: List[str]) -> List[Dict[str, Any]]:
    """Return list of files with metadata under base_path matching any prefix in pathSuffix."""
    results: List[Dict[str, Any]] = []
    try:
        level1 = webhdfs_list(base_path).get("FileStatuses", {}).get("FileStatus", [])
    except Exception:
        return results
    for ent in level1:
        name = ent.get("pathSuffix")
        full = f"{base_path}/{name}"
        if ent.get("type") == "FILE":
            if any(name.startswith(p) for p in prefix_filters):
                ent["fullPath"] = full
                results.append(ent)
        elif ent.get("type") == "DIRECTORY":
            results.extend(list_recursive_files(full, prefix_filters))
    return results


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "ts": str(int(time.time()))}


@app.get("/metrics/temperature")
def temperature(city: str | None = None, country: str | None = None, limit: int = DEFAULT_LIMIT):
    files = list_recursive_files(WEATHER_DATA_PATH, ["weather_"])
    files.sort(key=lambda f: f.get("modificationTime", 0), reverse=True)
    rows: List[Dict[str, Any]] = []
    for f in files[: max(limit, 1) * 5]:
        try:
            txt = webhdfs_open(f["fullPath"])
            data = requests.utils.json.loads(txt)
            rec_city = data.get("city", "")
            rec_country = data.get("country", "")
            if city and rec_city != city:
                continue
            if country and rec_country != country:
                continue
            rows.append({
                "event_time": data.get("weather", {}).get("time") or data.get("timestamp"),
                "temperature": data.get("weather", {}).get("temperature"),
                "city": rec_city,
                "country": rec_country,
            })
            if len(rows) >= limit:
                break
        except Exception:
            continue
    rows.sort(key=lambda r: str(r.get("event_time")))
    return JSONResponse(content=rows)


@app.get("/metrics/windspeed")
def windspeed(city: str | None = None, country: str | None = None, limit: int = DEFAULT_LIMIT):
    files = list_recursive_files(WEATHER_DATA_PATH, ["weather_"])
    files.sort(key=lambda f: f.get("modificationTime", 0), reverse=True)
    rows: List[Dict[str, Any]] = []
    for f in files[: max(limit, 1) * 5]:
        try:
            txt = webhdfs_open(f["fullPath"])
            data = requests.utils.json.loads(txt)
            rec_city = data.get("city", "")
            rec_country = data.get("country", "")
            if city and rec_city != city:
                continue
            if country and rec_country != country:
                continue
            rows.append({
                "event_time": data.get("weather", {}).get("time") or data.get("timestamp"),
                "windspeed": data.get("weather", {}).get("windspeed"),
                "city": rec_city,
                "country": rec_country,
            })
            if len(rows) >= limit:
                break
        except Exception:
            continue
    rows.sort(key=lambda r: str(r.get("event_time")))
    return JSONResponse(content=rows)


@app.get("/metrics/alerts/counts")
def alert_counts(country: str | None = None, city: str | None = None, limit: int = 500):
    files = list_recursive_files(ALERTS_PATH, ["alerts_"])
    files.sort(key=lambda f: f.get("modificationTime", 0), reverse=True)
    counts = {
        "wind": {"level_1": 0, "level_2": 0},
        "heat": {"level_1": 0, "level_2": 0},
    }
    for f in files[: max(limit, 1)]:
        try:
            txt = webhdfs_open(f["fullPath"]).strip()
            if not txt:
                continue
            data = requests.utils.json.loads(txt)
            if country and data.get("country") != country:
                continue
            if city and data.get("city") != city:
                continue
            if data.get("wind_alert_level") in ("level_1", "level_2"):
                counts["wind"][data["wind_alert_level"]] += 1
            if data.get("heat_alert_level") in ("level_1", "level_2"):
                counts["heat"][data["heat_alert_level"]] += 1
        except Exception:
            continue
    return JSONResponse(content=counts)


@app.get("/metrics/weathercode/top")
def weathercode_top(country: str | None = None, limit: int = 1000):
    files = list_recursive_files(WEATHER_DATA_PATH, ["weather_"])
    files.sort(key=lambda f: f.get("modificationTime", 0), reverse=True)
    counts: Dict[str, int] = {}
    for f in files[: max(limit, 1)]:
        try:
            txt = webhdfs_open(f["fullPath"]).strip()
            if not txt:
                continue
            data = requests.utils.json.loads(txt)
            if country and data.get("country") != country:
                continue
            code = str(data.get("weather", {}).get("weathercode"))
            if code and code != "None":
                counts[code] = counts.get(code, 0) + 1
        except Exception:
            continue
    by_country = {
    }
    return JSONResponse(content={"counts": counts})
