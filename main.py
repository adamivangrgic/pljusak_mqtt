#!/usr/bin/env python3
"""
Standalone Pljusak station scraper + MQTT reporter for Home Assistant.

Fetches https://pljusak.com/, parses all station readings, aggregates the
ones within `radius_km` of your location, and publishes to HA via MQTT
discovery. No radar, no webserver, no archive, no tracking.

Config: CLI flags override env vars, which override defaults.
  --location / LOCATION      City name ("Zadar") or "lat,lon"
  --lat/--lon / LAT,LON      Alternative to --location
  --radius-km / RADIUS_KM    Aggregation radius (default 5)
  --poll-s / POLL_S          Poll interval in seconds (default 60)
  --max-age-min / MAX_AGE_MIN  Station freshness threshold (default 15)
  --gust-mode / GUST_MODE    max|median|mean (default max)
  MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASSWORD, MQTT_TLS
  --device-id / DEVICE_ID, --device-name / DEVICE_NAME
  --dry-run / DRY_RUN        Fetch + aggregate + log, no MQTT publish
"""

import argparse
import json
import math
import os
import re
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import requests

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: paho-mqtt required. pip install paho-mqtt", file=sys.stderr)
    sys.exit(1)

try:
    from zoneinfo import ZoneInfo
    HAVE_ZONEINFO = True
except ImportError:
    HAVE_ZONEINFO = False


# ============================================================================
# Constants
# ============================================================================

PLJUSAK_URL = "https://pljusak.com/"
PLJUSAK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (pljusak-station-puller/2.0)",
    "Referer": "https://pljusak.com/",
    "Accept": "text/html,application/xhtml+xml",
}

COMPASS_16_DEG = {
    "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
    "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
    "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
    "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
}

CITIES = [
    ("zg", "Zagreb",         45.8146, 15.9722),
    ("ri", "Rijeka",         45.3271, 14.4422),
    ("st", "Split",          43.5081, 16.4402),
    ("os", "Osijek",         45.5550, 18.6955),
    ("zd", "Zadar",          44.1194, 15.2314),
    ("db", "Dubrovnik",      42.6507, 18.0944),
    ("pu", "Pula",           44.8666, 13.8496),
    ("ka", "Karlovac",       45.4929, 15.5551),
    ("va", "Varaždin",       46.3057, 16.3366),
    ("lj", "Ljubljana",      46.0569, 14.5058),
    ("tb", "Sarajevo",       43.8633, 18.4131),
    ("sk", "Sisak",          45.4667, 16.3789),
    ("mk", "Makarska",       43.2969, 17.0178),
    ("šb", "Šibenik",        43.7350, 15.8956),
    ("vk", "Vukovar",        45.2551, 19.1122),
    ("sb", "Slavonski Brod", 45.1603, 18.0156),
    ("po", "Požega",         45.3403, 17.6853),
    ("bj", "Bjelovar",       45.8986, 16.8422),
    ("kt", "Kutina",         45.4772, 16.7783),
    ("gs", "Gospić",         44.5461, 15.3747),
    ("kn", "Knin",           44.0409, 16.1969),
]


# ============================================================================
# Helpers
# ============================================================================

def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def bearing_label(deg):
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return dirs[int((deg + 11.25) % 360 // 22.5)]


def get_zoneinfo(name):
    if not HAVE_ZONEINFO:
        return timezone.utc
    try:
        return ZoneInfo(name or "UTC")
    except Exception:
        return timezone.utc


def resolve_location(location, lat, lon):
    """Return (name, lat, lon). Accepts --lat/--lon, or --location as city
    name ('Zadar') or 'lat,lon' string."""
    if lat is not None and lon is not None:
        return f"({lat:.4f},{lon:.4f})", lat, lon
    if not location:
        return None
    if "," in location:
        try:
            a, b = location.split(",", 1)
            return f"({float(a):.4f},{float(b):.4f})", float(a), float(b)
        except ValueError:
            pass
    import unicodedata
    def norm(s):
        s = unicodedata.normalize("NFKD", s)
        return "".join(c for c in s if not unicodedata.combining(c)).lower().strip()
    q = norm(location)
    for label, name, c_lat, c_lon in CITIES:
        if q == norm(name) or q == norm(label):
            return name, c_lat, c_lon
    raise SystemExit(f"Unknown location '{location}'. Use --lat/--lon for "
                     f"locations not in the built-in list.")


def _parse_opt_float(row, idx, lo=None, hi=None):
    if len(row) <= idx:
        return None
    val = row[idx]
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip().replace(",", ".")
        if not s or s in ("-", "—", "null", "None", "NaN", "nan"):
            return None
        try:
            f = float(s)
        except ValueError:
            return None
    else:
        try:
            f = float(val)
        except (TypeError, ValueError):
            return None
    if not math.isfinite(f):
        return None
    if lo is not None and f < lo:
        return None
    if hi is not None and f > hi:
        return None
    return f


def _parse_wind_dir_deg(raw_dir):
    if raw_dir is None:
        return None, None
    s = str(raw_dir).strip().upper()
    if not s or s in ("-", "—", "TIHO", "CALM", "C", "VRB", "0"):
        return s if s and s != "-" else None, None
    if s in COMPASS_16_DEG:
        return s, COMPASS_16_DEG[s]
    try:
        deg = float(s)
        if math.isfinite(deg) and 0.0 <= deg <= 360.0:
            deg = deg % 360.0
            return bearing_label(deg), deg
    except ValueError:
        pass
    return s, None


def _station_age_min(time_str, fetch_utc, display_tz):
    if not time_str or not fetch_utc:
        return None
    try:
        parts = time_str.split(":")
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    except (ValueError, IndexError):
        return None
    fetch_local = fetch_utc.astimezone(display_tz)
    dt = datetime(fetch_local.year, fetch_local.month, fetch_local.day,
                  h, m, s, tzinfo=display_tz)
    if dt > fetch_local + timedelta(minutes=2):
        dt -= timedelta(days=1)
    age = (fetch_local - dt).total_seconds() / 60.0
    return None if (age < 0 or age > 24 * 60) else age


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class StationReading:
    slug: str
    name: str
    lat: float
    lon: float
    precip_1h: float
    time_str: str
    elevation_m: Optional[float] = None
    temp_c: Optional[float] = None
    temp_trend: Optional[float] = None
    pressure_hpa: Optional[float] = None
    pressure_trend: Optional[float] = None
    humidity_pct: Optional[float] = None
    wind_dir_str: Optional[str] = None
    wind_dir_deg: Optional[float] = None
    wind_speed_kmh: Optional[float] = None
    wind_gust_kmh: Optional[float] = None
    precip_month: Optional[float] = None
    precip_today: Optional[float] = None
    precip_24h: Optional[float] = None
    precip_6h: Optional[float] = None
    max_temp_c: Optional[float] = None
    min_temp_c: Optional[float] = None
    uv: Optional[float] = None
    temp_24h_c: Optional[float] = None


@dataclass
class StationAnalysis:
    reading: StationReading
    dist_km: float
    age_min: Optional[float]
    slope_mmh: Optional[float]
    trend: str
    rain_rate_mmh: Optional[float] = None


@dataclass
class LocalWeather:
    radius_km: float = 5.0
    station_count: int = 0
    nearest_station: Optional[str] = None
    nearest_dist_km: Optional[float] = None
    temp_c: Optional[float] = None
    temp_trend: Optional[float] = None
    humidity_pct: Optional[float] = None
    pressure_hpa: Optional[float] = None
    pressure_trend: Optional[float] = None
    wind_speed_kmh: Optional[float] = None
    wind_gust_kmh: Optional[float] = None
    wind_dir_deg: Optional[int] = None
    wind_dir_cardinal: Optional[str] = None
    precip_today_mm: Optional[float] = None
    precip_1h_mm: Optional[float] = None
    precip_6h_mm: Optional[float] = None
    precip_24h_mm: Optional[float] = None
    rain_rate_mmh: Optional[float] = None
    rain_trend: Optional[str] = None


# ============================================================================
# Pljusak client
# ============================================================================

class PljusakClient:
    def __init__(self, url=PLJUSAK_URL, timeout=30):
        self.url = url
        self.timeout = timeout
        self.history = {}
        self.last_fetch_utc = None
        self.last_readings = []
        self.last_error = None

    def fetch(self, now_utc):
        try:
            r = requests.get(self.url, headers=PLJUSAK_HEADERS, timeout=self.timeout)
            r.raise_for_status()
            m = re.search(r"var\s+podaci\s*=\s*(\[.*?\]);\s*</script>", r.text, re.S)
            if not m:
                self.last_error = "regex miss"
                return False
            rows = json.loads(m.group(1))
            readings = []
            for row in rows:
                if not row or row[0] == "kamera":
                    continue
                lat = _parse_opt_float(row, 2, -90.0, 90.0)
                lon = _parse_opt_float(row, 3, -180.0, 180.0)
                if lat is None or lon is None:
                    continue
                elev_m = _parse_opt_float(row, 4, -50.0, 5000.0)
                ts = str(row[10]).strip() if len(row) > 10 and row[10] is not None else ""
                temp_c = _parse_opt_float(row, 12, -55.0, 60.0)
                temp_trend = _parse_opt_float(row, 13, -30.0, 30.0)
                pressure_hpa = _parse_opt_float(row, 14, 930.0, 1070.0)
                pressure_trend = _parse_opt_float(row, 15, -30.0, 30.0)
                humidity_pct = _parse_opt_float(row, 16, 1.0, 100.0)
                raw_dir = row[17] if len(row) > 17 else None
                wind_dir_str, wind_dir_deg = _parse_wind_dir_deg(raw_dir)
                wind_ms = _parse_opt_float(row, 18, 0.0, 90.0)
                wind_speed_kmh = (wind_ms * 3.6) if wind_ms is not None else None
                precip_month = _parse_opt_float(row, 19, 0.0, 5000.0)
                precip_today = _parse_opt_float(row, 20, 0.0, 1500.0)
                precip_24h = _parse_opt_float(row, 21, 0.0, 1500.0)
                p1h_opt = _parse_opt_float(row, 22, 0.0, 500.0)
                precip_1h = p1h_opt if p1h_opt is not None else 0.0
                precip_6h = _parse_opt_float(row, 23, 0.0, 1000.0)
                max_temp_c = _parse_opt_float(row, 24, -55.0, 60.0)
                min_temp_c = _parse_opt_float(row, 25, -55.0, 60.0)
                gust_ms = _parse_opt_float(row, 26, 0.0, 110.0)
                wind_gust_kmh = (gust_ms * 3.6) if gust_ms is not None else None
                uv = _parse_opt_float(row, 27, 0.0, 25.0)
                temp_24h_c = _parse_opt_float(row, 28, -55.0, 60.0)
                slug = str(row[29]).strip() if len(row) > 29 and row[29] is not None else ""
                name = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
                readings.append(StationReading(
                    slug=slug, name=name, lat=lat, lon=lon,
                    precip_1h=precip_1h, time_str=ts,
                    elevation_m=elev_m, temp_c=temp_c, temp_trend=temp_trend,
                    pressure_hpa=pressure_hpa, pressure_trend=pressure_trend,
                    humidity_pct=humidity_pct, wind_dir_str=wind_dir_str,
                    wind_dir_deg=wind_dir_deg, wind_speed_kmh=wind_speed_kmh,
                    wind_gust_kmh=wind_gust_kmh, precip_month=precip_month,
                    precip_today=precip_today, precip_24h=precip_24h,
                    precip_6h=precip_6h, max_temp_c=max_temp_c,
                    min_temp_c=min_temp_c, uv=uv, temp_24h_c=temp_24h_c))
            self.last_readings = readings
            self.last_fetch_utc = now_utc
            self.last_error = None
            for st in readings:
                h = self.history.setdefault(st.slug, [])
                h.append((now_utc, st.precip_1h, st.precip_today))
                if len(h) > 60:
                    del h[:len(h) - 60]
            return True
        except Exception as e:
            self.last_error = str(e)
            return False

    def analyze_nearby(self, t_lat, t_lon, radius_km, display_tz):
        out = []
        for st in self.last_readings:
            dist = haversine_km(st.lat, st.lon, t_lat, t_lon)
            if radius_km is not None and dist > radius_km:
                continue
            age = _station_age_min(st.time_str, self.last_fetch_utc, display_tz)
            slope, trend = self.slope_mmh(st.slug, window_min=12)
            rate = self.rain_rate_mmh(st.slug, window_min=12)
            out.append(StationAnalysis(st, dist, age, slope, trend, rain_rate_mmh=rate))
        out.sort(key=lambda a: a.dist_km)
        return out

    def slope_mmh(self, slug, window_min=12):
        h = self.history.get(slug)
        if not h or len(h) < 2:
            return None, "insufficient"
        latest_ts, latest_mm, _ = h[-1]
        earliest = None
        for entry in reversed(h):
            if (latest_ts - entry[0]).total_seconds() >= window_min * 60:
                earliest = entry
                break
        if not earliest:
            return None, "insufficient"
        dt_h = (latest_ts - earliest[0]).total_seconds() / 3600.0
        if dt_h <= 0:
            return None, "insufficient"
        slope = (latest_mm - earliest[1]) / dt_h
        if slope > 50 or slope < -50:
            return None, "unknown"
        if slope > 0.5:
            return slope, "rising"
        if slope < -0.5:
            return slope, "falling"
        return slope, "stable"

    def rain_rate_mmh(self, slug, window_min=12):
        h = self.history.get(slug)
        if not h or len(h) < 2:
            return None
        latest_ts, latest_1h, latest_today = h[-1]
        earliest = None
        for entry in reversed(h[:-1]):
            if (latest_ts - entry[0]).total_seconds() >= window_min * 60:
                earliest = entry
                break
        if earliest is None:
            earliest = h[0]
        dt_h = (latest_ts - earliest[0]).total_seconds() / 3600.0
        if dt_h < (3.0 / 60.0):
            return None
        earliest_1h, earliest_today = earliest[1], earliest[2]
        if (latest_today is not None and earliest_today is not None
                and latest_today >= earliest_today):
            rate = (latest_today - earliest_today) / dt_h
        else:
            rate = max(0.0, (latest_1h - earliest_1h) / dt_h)
        if not math.isfinite(rate) or rate < 0.0 or rate > 250.0:
            return None
        return rate


# ============================================================================
# Aggregation
# ============================================================================

def summarize_local_weather(stations, radius_km, max_age_min, gust_mode="max"):
    lw = LocalWeather(radius_km=float(radius_km))
    fresh = [s for s in stations
             if s.dist_km <= radius_km
             and s.age_min is not None
             and s.age_min <= max_age_min]
    lw.station_count = len(fresh)
    if not fresh:
        return lw
    nearest = min(fresh, key=lambda s: s.dist_km)
    lw.nearest_station = nearest.reading.name or nearest.reading.slug
    lw.nearest_dist_km = round(nearest.dist_km, 1)

    def _med(vals, digits=1):
        clean = [v for v in vals if v is not None and math.isfinite(v)]
        if not clean:
            return None
        m = float(np.median(clean))
        return round(m, digits) if digits is not None else m

    lw.temp_c = _med([s.reading.temp_c for s in fresh], 1)
    lw.temp_trend = _med([s.reading.temp_trend for s in fresh], 2)
    lw.humidity_pct = _med([s.reading.humidity_pct for s in fresh], 0)
    if lw.humidity_pct is not None:
        lw.humidity_pct = int(round(lw.humidity_pct))
    lw.pressure_hpa = _med([s.reading.pressure_hpa for s in fresh], 1)
    lw.pressure_trend = _med([s.reading.pressure_trend for s in fresh], 2)
    lw.wind_speed_kmh = _med([s.reading.wind_speed_kmh for s in fresh], 1)

    # Gust aggregation: max = "worst nearby peak" (default), median = "typical
    # peak", mean = "average peak". Max is the right default for gust warnings;
    # median is consistent with wind_speed_kmh.
    gusts = [s.reading.wind_gust_kmh for s in fresh
             if s.reading.wind_gust_kmh is not None]
    if gusts:
        if gust_mode == "median":
            lw.wind_gust_kmh = round(float(np.median(gusts)), 1)
        elif gust_mode == "mean":
            lw.wind_gust_kmh = round(float(np.mean(gusts)), 1)
        else:
            lw.wind_gust_kmh = round(float(max(gusts)), 1)

    sin_sum = 0.0
    cos_sum = 0.0
    dir_count = 0
    for s in fresh:
        deg = s.reading.wind_dir_deg
        if deg is None:
            continue
        spd = s.reading.wind_speed_kmh
        w = max(0.5, float(spd)) if spd is not None else 1.0
        rad = math.radians(deg)
        sin_sum += w * math.sin(rad)
        cos_sum += w * math.cos(rad)
        dir_count += 1
    if dir_count > 0 and math.hypot(sin_sum, cos_sum) > 1e-6:
        mean_deg = math.degrees(math.atan2(sin_sum, cos_sum)) % 360.0
        deg_int = int(round(mean_deg)) % 360
        lw.wind_dir_deg = deg_int
        lw.wind_dir_cardinal = bearing_label(deg_int)

    lw.precip_today_mm = _med([s.reading.precip_today for s in fresh], 1)
    lw.precip_1h_mm = _med([s.reading.precip_1h for s in fresh], 1)
    lw.precip_6h_mm = _med([s.reading.precip_6h for s in fresh], 1)
    lw.precip_24h_mm = _med([s.reading.precip_24h for s in fresh], 1)
    lw.rain_rate_mmh = _med([s.rain_rate_mmh for s in fresh], 1)

    # Rain trend: majority-vote of station trends
    trend_counts = {"rising": 0, "falling": 0, "stable": 0}
    for s in fresh:
        if s.trend in trend_counts:
            trend_counts[s.trend] += 1
    if any(trend_counts.values()):
        lw.rain_trend = max(trend_counts, key=trend_counts.get)
    return lw


# ============================================================================
# MQTT
# ============================================================================

SENSOR_DEFS = [
    ("local_temp_c",              "Temperature",        "sensor", "temperature", "°C", None, None),
    ("local_temp_trend",          "Temperature trend",  "sensor", None, "°C/h", "mdi:thermometer-trending-up", "diagnostic"),
    ("local_humidity",            "Humidity",           "sensor", "humidity", "%", None, None),
    ("local_pressure_hpa",        "Pressure",           "sensor", "atmospheric_pressure", "hPa", None, None),
    ("local_pressure_trend",      "Pressure trend",     "sensor", None, "hPa/h", "mdi:gauge", "diagnostic"),
    ("local_wind_speed_kmh",      "Wind speed",         "sensor", "wind_speed", "km/h", None, None),
    ("local_wind_gust_kmh",       "Wind gust",          "sensor", "wind_speed", "km/h", "mdi:weather-windy", None),
    ("local_wind_dir_deg",        "Wind bearing",       "sensor", "wind_direction", "°", None, None),
    ("local_wind_dir",            "Wind direction",     "sensor", None, None, "mdi:compass-outline", None),
    ("local_rain_today_mm",       "Rain today",         "sensor", "precipitation", "mm", "mdi:weather-rainy", None),
    ("local_rain_1h_mm",          "Rain 1h",            "sensor", "precipitation", "mm", "mdi:weather-rainy", None),
    ("local_rain_6h_mm",          "Rain 6h",            "sensor", "precipitation", "mm", "mdi:weather-rainy", None),
    ("local_rain_24h_mm",         "Rain 24h",           "sensor", "precipitation", "mm", "mdi:weather-rainy", None),
    ("local_rain_rate_mmh",       "Rain rate",          "sensor", "precipitation_intensity", "mm/h", "mdi:weather-pouring", None),
    ("local_rain_trend",          "Rain trend",         "sensor", None, None, "mdi:trending-up", "diagnostic"),
    ("local_station_count",       "Stations nearby",    "sensor", None, None, "mdi:weather-station", "diagnostic"),
    ("local_nearest_station",     "Nearest station",    "sensor", None, None, "mdi:map-marker", "diagnostic"),
    ("local_nearest_distance_km", "Nearest distance",   "sensor", "distance", "km", "mdi:map-marker-distance", "diagnostic"),
]
SENSOR_KEYS = [s[0] for s in SENSOR_DEFS]


class HAMQTTBridge:
    def __init__(self, host, port=1883, user=None, password=None,
                 discovery_prefix="homeassistant", state_prefix="pljusak",
                 device_id="pljusak_local_weather",
                 device_name="Pljusak Local Weather",
                 retain=True, tls=False, web_url=None):
        self.host = host
        self.port = port
        self.discovery_prefix = discovery_prefix
        self.state_prefix = state_prefix
        self.device_id = device_id
        self.device_name = device_name
        self.retain = retain
        self.avail_topic = f"{state_prefix}/{device_id}/status"
        self.web_url = web_url
        self._discovered = False
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                   client_id=f"{device_id}-{os.getpid()}")
        if user:
            self._client.username_pw_set(user, password)
        if tls:
            self._client.tls_set()
        self._client.will_set(self.avail_topic, "offline", qos=1, retain=True)
        self._client.on_connect = self._on_connect

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        client.publish(self.avail_topic, "online", qos=1, retain=True)
        if not self._discovered:
            self.publish_discovery()

    def connect(self):
        self._client.connect(self.host, self.port, keepalive=60)
        self._client.loop_start()

    def close(self):
        try:
            self._client.publish(self.avail_topic, "offline", qos=1, retain=True)
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass

    def _device_block(self):
        d = {"identifiers": [self.device_id], "name": self.device_name,
             "manufacturer": "pljusak.com", "model": "Station aggregator"}
        if self.web_url:
            d["configuration_url"] = self.web_url
        return d

    def _state_topic(self, key):
        return f"{self.state_prefix}/{self.device_id}/{key}/state"

    def _attr_topic(self, key):
        return f"{self.state_prefix}/{self.device_id}/{key}/attributes"

    def publish_discovery(self):
        dev = self._device_block()
        for key, name, kind, dev_cls, unit, icon, ent_cat in SENSOR_DEFS:
            uid = f"{self.device_id}_{key}"
            disc_topic = f"{self.discovery_prefix}/sensor/{self.device_id}/{key}/config"
            payload = {"name": name, "unique_id": uid, "device": dev,
                       "availability_topic": self.avail_topic,
                       "payload_available": "online", "payload_not_available": "offline",
                       "state_topic": self._state_topic(key)}
            if dev_cls: payload["device_class"] = dev_cls
            if unit: payload["unit_of_measurement"] = unit
            if icon: payload["icon"] = icon
            if ent_cat: payload["entity_category"] = ent_cat
            if key in ("local_temp_c", "local_rain_today_mm", "local_wind_speed_kmh"):
                payload["json_attributes_topic"] = self._attr_topic(key)
            self._client.publish(disc_topic, json.dumps(payload), qos=1, retain=True)
        self._discovered = True

    def publish_state(self, state_dict, attrs_by_key=None):
        attrs_by_key = attrs_by_key or {}
        for key, name, kind, *_ in SENSOR_DEFS:
            val = state_dict.get(key)
            payload = "unknown" if val is None else str(val)
            self._client.publish(self._state_topic(key), payload, qos=0, retain=self.retain)
            if key in attrs_by_key:
                self._client.publish(self._attr_topic(key),
                                     json.dumps(attrs_by_key[key], default=str),
                                     qos=0, retain=self.retain)


# ============================================================================
# Config
# ============================================================================

def _env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _env_bool(name, default=False):
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    return v.lower() in ("1", "true", "yes", "on")


def build_config():
    p = argparse.ArgumentParser(description="Pljusak → Home Assistant via MQTT")
    p.add_argument("--location", default=_env("LOCATION"))
    p.add_argument("--lat", type=float, default=float(_env("LAT", "nan")) if _env("LAT") else None)
    p.add_argument("--lon", type=float, default=float(_env("LON", "nan")) if _env("LON") else None)
    p.add_argument("--radius-km", type=float,
                   default=float(_env("RADIUS_KM", "5")))
    p.add_argument("--poll-s", type=int, default=int(_env("POLL_S", "60")))
    p.add_argument("--max-age-min", type=float,
                   default=float(_env("MAX_AGE_MIN", "15")))
    p.add_argument("--gust-mode", choices=("max", "median", "mean"),
                   default=_env("GUST_MODE", "max"))
    p.add_argument("--display-tz", default=_env("DISPLAY_TZ", "Europe/Zagreb"))

    p.add_argument("--mqtt-host", default=_env("MQTT_HOST"))
    p.add_argument("--mqtt-port", type=int, default=int(_env("MQTT_PORT", "1883")))
    p.add_argument("--mqtt-user", default=_env("MQTT_USER"))
    p.add_argument("--mqtt-password", default=_env("MQTT_PASSWORD"))
    p.add_argument("--mqtt-tls", action=argparse.BooleanOptionalAction,
                   default=_env_bool("MQTT_TLS", False))
    p.add_argument("--mqtt-discovery-prefix",
                   default=_env("MQTT_DISCOVERY_PREFIX", "homeassistant"))
    p.add_argument("--mqtt-state-prefix", default=_env("MQTT_STATE_PREFIX", "pljusak"))
    p.add_argument("--device-id", default=_env("DEVICE_ID", "pljusak_local_weather"))
    p.add_argument("--device-name", default=_env("DEVICE_NAME", "Pljusak Local Weather"))

    p.add_argument("--dry-run", action="store_true", default=_env_bool("DRY_RUN", False))
    p.add_argument("--once", action="store_true", help="Poll once and exit")
    p.add_argument("--log-level", choices=("quiet", "info", "debug"),
                   default=_env("LOG_LEVEL", "info"))
    return p.parse_args()


# ============================================================================
# Main
# ============================================================================

def _log(level, msg, cfg_level):
    if cfg_level == "quiet":
        return
    if cfg_level == "info" and level == "debug":
        return
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {level.upper():5s} {msg}", flush=True)


def build_state_dict(lw, target_name):
    return {
        "local_temp_c": lw.temp_c,
        "local_temp_trend": lw.temp_trend,
        "local_humidity": lw.humidity_pct,
        "local_pressure_hpa": lw.pressure_hpa,
        "local_pressure_trend": lw.pressure_trend,
        "local_wind_speed_kmh": lw.wind_speed_kmh,
        "local_wind_gust_kmh": lw.wind_gust_kmh,
        "local_wind_dir_deg": lw.wind_dir_deg,
        "local_wind_dir": lw.wind_dir_cardinal,
        "local_rain_today_mm": lw.precip_today_mm,
        "local_rain_1h_mm": lw.precip_1h_mm,
        "local_rain_6h_mm": lw.precip_6h_mm,
        "local_rain_24h_mm": lw.precip_24h_mm,
        "local_rain_rate_mmh": lw.rain_rate_mmh,
        "local_rain_trend": lw.rain_trend,
        "local_station_count": lw.station_count,
        "local_nearest_station": lw.nearest_station,
        "local_nearest_distance_km": lw.nearest_dist_km,
    }


def build_attrs(lw, target_name, gust_mode):
    return {
        "local_temp_c": {
            "target": target_name,
            "radius_km": lw.radius_km,
            "station_count": lw.station_count,
            "temp_trend_c_h": lw.temp_trend,
            "pressure_trend_hpa_h": lw.pressure_trend,
        },
        "local_wind_speed_kmh": {
            "gust_mode": gust_mode,
            "nearest_station": lw.nearest_station,
            "nearest_dist_km": lw.nearest_dist_km,
            "wind_dir_cardinal": lw.wind_dir_cardinal,
            "wind_dir_deg": lw.wind_dir_deg,
        },
        "local_rain_today_mm": {
            "rain_1h_mm": lw.precip_1h_mm,
            "rain_6h_mm": lw.precip_6h_mm,
            "rain_24h_mm": lw.precip_24h_mm,
            "rain_rate_mmh": lw.rain_rate_mmh,
            "rain_trend": lw.rain_trend,
        },
    }


def main():
    cfg = build_config()

    # Resolve target
    target = resolve_location(cfg.location, cfg.lat, cfg.lon)
    if target is None:
        print("ERROR: --location or --lat/--lon required", file=sys.stderr)
        sys.exit(1)
    target_name, target_lat, target_lon = target
    display_tz = get_zoneinfo(cfg.display_tz)

    _log("info", f"target: {target_name} ({target_lat:.4f}, {target_lon:.4f}) "
                 f"radius={cfg.radius_km}km poll={cfg.poll_s}s "
                 f"gust_mode={cfg.gust_mode} tz={cfg.display_tz}", cfg.log_level)

    # MQTT (skipped in dry-run)
    bridge = None
    if not cfg.dry_run:
        if not cfg.mqtt_host:
            print("ERROR: MQTT_HOST required (or use --dry-run)", file=sys.stderr)
            sys.exit(1)
        try:
            bridge = HAMQTTBridge(
                host=cfg.mqtt_host, port=cfg.mqtt_port,
                user=cfg.mqtt_user, password=cfg.mqtt_password,
                discovery_prefix=cfg.mqtt_discovery_prefix,
                state_prefix=cfg.mqtt_state_prefix,
                device_id=cfg.device_id, device_name=cfg.device_name,
                retain=True, tls=cfg.mqtt_tls)
            bridge.connect()
            _log("info", f"MQTT: {cfg.mqtt_host}:{cfg.mqtt_port} "
                         f"({cfg.device_id}, prefix={cfg.mqtt_state_prefix})", cfg.log_level)
        except Exception as e:
            print(f"ERROR: MQTT connect failed: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        _log("info", "DRY RUN — no MQTT publish", cfg.log_level)

    # Pljusak
    client = PljusakClient()

    # Graceful shutdown on SIGTERM (docker stop) + SIGINT (Ctrl-C)
    stop_flag = {"stop": False}
    def _signal(sig, _frame):
        stop_flag["stop"] = True
    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGINT, _signal)

    try:
        while not stop_flag["stop"]:
            t_start = time.monotonic()
            now_utc = datetime.now(timezone.utc)

            ok = client.fetch(now_utc)
            if not ok:
                _log("info", f"fetch failed: {client.last_error}", cfg.log_level)
                if cfg.once:
                    break
                _sleep(cfg.poll_s, t_start, stop_flag)
                continue

            stations = client.analyze_nearby(target_lat, target_lon,
                                             cfg.radius_km, display_tz)
            lw = summarize_local_weather(stations, cfg.radius_km,
                                         cfg.max_age_min, gust_mode=cfg.gust_mode)
            state = build_state_dict(lw, target_name)
            attrs = build_attrs(lw, target_name, cfg.gust_mode)

            if cfg.log_level != "quiet":
                _log("info",
                     f"{lw.station_count} fresh · "
                     f"T={_fmt(lw.temp_c)}°C "
                     f"RH={_fmt(lw.humidity_pct)}% "
                     f"P={_fmt(lw.pressure_hpa)}hPa "
                     f"W={_fmt(lw.wind_speed_kmh)}km/h "
                     f"G={_fmt(lw.wind_gust_kmh)}km/h "
                     f"R1h={_fmt(lw.precip_1h_mm)}mm "
                     f"rate={_fmt(lw.rain_rate_mmh)}mm/h",
                     cfg.log_level)

            if bridge:
                bridge.publish_state(state, attrs_by_key=attrs)

            if cfg.once:
                break
            _sleep(cfg.poll_s, t_start, stop_flag)
    finally:
        if bridge:
            bridge.close()
            _log("info", "MQTT closed", cfg.log_level)


def _sleep(seconds, t_start, stop_flag):
    """Sleep `seconds` minus elapsed time, waking every 1s to check stop_flag."""
    elapsed = time.monotonic() - t_start
    remaining = max(0.0, seconds - elapsed)
    deadline = time.monotonic() + remaining
    while time.monotonic() < deadline and not stop_flag["stop"]:
        time.sleep(min(1.0, deadline - time.monotonic()))


def _fmt(v):
    return "—" if v is None else f"{v:g}"


if __name__ == "__main__":
    main()
