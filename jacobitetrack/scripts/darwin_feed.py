#!/usr/bin/env python3
"""Consume rail feeds and publish estimated train positions for the map line.

The Kafka credentials are read only from the GitHub Actions environment. This
module deliberately emits no train coordinates: Darwin reports station times,
not continuous GPS positions.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
FEED_PATH = ROOT / "jacobitetrack" / "feed.json"
STATE_PATH = ROOT / ".github" / "rail-feed-state.json"
LONDON = ZoneInfo("Europe/London")
STATION_BY_TIPLOC = {
    # Darwin's TIPLOCs are not always the station name or CRS code.
    # Cumulative straight-line distance between map points (km). These are
    # route-distance proxies, not Network Rail chainage or GPS coordinates.
    "MLAIG": ("mallaig", 0.0),
    "MORAR": ("morar", 4.108),
    "ARISAIG": ("arisaig", 10.325),
    # Beasdale is retained as an unlabelled timing anchor between map stations.
    "BEASDAL": ("beasdale", 15.308),
    "LCHALRT": ("lochailort", 21.679),
    "GLNFNNN": ("glenfinnan", 34.743),
    "LCHESDE": ("locheilside", 44.546),
    "LCEILOB": ("lochEilOutwardBound", 50.468),
    "FRTWLM": ("fortWilliam", 59.214),
    # Accept descriptive variants in case another Darwin version emits them.
    "MALLAIG": ("mallaig", 0.0),
    "LOCHAILORT": ("lochailort", 21.679),
    "GLENFINNAN": ("glenfinnan", 34.743),
    "FTWILLIAM": ("fortWilliam", 59.214),
    "FORTWILLIAM": ("fortWilliam", 59.214),
}
TIPLOC_BY_CRS = {
    "MLG": "MLAIG", "MRR": "MORAR", "ARG": "ARISAIG", "BSL": "BEASDAL",
    "LCL": "LCHALRT", "GLF": "GLNFNNN", "LCS": "LCHESDE",
    "LHE": "LCEILOB", "FTW": "FRTWLM",
}
LDB_STATION_CRSES = tuple(crs for crs in TIPLOC_BY_CRS if crs != "BSL")
STATION_NAME_BY_KEY = {
    "mallaig": "Mallaig",
    "morar": "Morar",
    "arisaig": "Arisaig",
    "beasdale": "Beasdale",
    "lochailort": "Lochailort",
    "glenfinnan": "Glenfinnan",
    "locheilside": "Locheilside",
    "lochEilOutwardBound": "Loch Eil Outward Bound",
    "fortWilliam": "Fort William",
}
VIEWPOINT_BY_KEY = {
    "mallaig": "mallaig",
    "morar": "morar",
    "arisaig": "arisaig",
    "lochailort": "lochailort",
    "glenfinnan": "glenfinnan",
    "locheilside": "locheilside",
    "lochEilOutwardBound": "lochEilOutwardBound",
    "fortWilliam": "fortWilliam",
}
LINE_TIPLOCS = set(STATION_BY_TIPLOC)
# Network Rail CORPUS STANOX values for the mapped line stations and the
# Beasdale timing point. These are reference IDs, not geographic coordinates.
STANOX_TO_TIPLOC = {
    "06001": "MLAIG", "06002": "MORAR", "06004": "ARISAIG",
    "06005": "BEASDAL", "06006": "LCHALRT", "06008": "GLNFNNN",
    "06009": "LCHESDE", "06014": "LCEILOB",
    "06023": "FRTWLM",
}
FRESH_SECONDS = 600
LINE_DISTANCE_KM = 59.214
# The viaduct is roughly 1.1 km before Glenfinnan station in the Mallaig
# direction, based on public map coordinates; this is a route-distance proxy.
VIADUCT_DISTANCE_KM = 33.65
SCHEDULE_HORIZON = timedelta(hours=48)


def _ldb_time(value: Any, now: datetime) -> datetime | None:
    """Parse an LDB local HH:mm time, choosing the nearest plausible service day."""
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo:
            return parsed.astimezone(timezone.utc)
    except ValueError:
        pass
    match = re.search(r"(\d{1,2}):(\d{2})", raw)
    if not match:
        digits = re.sub(r"\D", "", raw)
        if len(digits) not in (3, 4):
            return None
        hour, minute = int(digits[:-2]), int(digits[-2:])
    else:
        hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59:
        return None
    local_now = now.astimezone(LONDON)
    candidates = [datetime.combine(local_now.date() + timedelta(days=offset), dt_time(hour, minute), LONDON)
                  for offset in (-1, 0, 1)]
    plausible = [candidate for candidate in candidates
                 if -timedelta(hours=12) <= candidate - local_now <= timedelta(hours=48)]
    if not plausible:
        plausible = candidates
    chosen = min(plausible, key=lambda candidate: abs((candidate - local_now).total_seconds()))
    return chosen.astimezone(timezone.utc)


def _ldb_calling_points(service: dict[str, Any], side: str) -> list[dict[str, Any]]:
    groups = get(service, side, default=[])
    points: list[dict[str, Any]] = []
    for group in as_list(groups):
        if isinstance(group, dict):
            points.extend(point for point in as_list(get(group, "callingPoint", default=[])) if isinstance(point, dict))
        elif isinstance(group, list):
            points.extend(point for point in group if isinstance(point, dict))
    return points


def _merge_ldb_service(state: dict[str, Any], raw: dict[str, Any], board_crs: str, now: datetime) -> bool:
    """Keep a departure-board service only when its calling points match this line."""
    board_tiploc = TIPLOC_BY_CRS.get(board_crs)
    if not board_tiploc:
        return False
    raw_id = str(get(raw, "serviceID", "trainID", "rid", "rsid", default="") or "").strip()
    origin = as_list(get(raw, "origin", default=[]))
    destination = as_list(get(raw, "destination", default=[]))
    description = " ".join(str(get(item, "locationName", "description", default="")) for item in origin + destination)
    id_part = raw_id or re.sub(r"\W+", "", description)
    # RDM LDB may return the same service once per departure board with a
    # station suffix (for example 10875321MLAIG___). Collapse those variants
    # to one service so its calling points combine into a single map marker.
    station_suffix = re.fullmatch(
        r"(\d{8})(?:MLAIG|MORAR|ARISAIG|BEASDAL|LCHALRT|GLNFNNN|LCHESDE|LCEILOB|FRTWLM)_*",
        id_part.upper(),
    )
    canonical_id = station_suffix.group(1) if station_suffix else id_part
    if not id_part:
        return False
    identifier = "ldb:" + canonical_id
    raw_points = _ldb_calling_points(raw, "previousCallingPoints") + [{
        "crs": board_crs,
        "st": get(raw, "std"),
        "et": get(raw, "etd"),
        "at": get(raw, "at"),
    }] + _ldb_calling_points(raw, "subsequentCallingPoints")
    locations: dict[str, dict[str, str]] = {}
    schedule_at: dict[str, str] = {}
    route: list[str] = []
    for point in raw_points:
        crs = str(get(point, "crs", default="") or "").upper()
        tiploc = TIPLOC_BY_CRS.get(crs)
        if not tiploc:
            continue
        at_time = _ldb_time(get(point, "at"), now)
        eta_time = _ldb_time(get(point, "et"), now)
        scheduled_time = _ldb_time(get(point, "st"), now)
        if scheduled_time:
            schedule_at[tiploc] = scheduled_time.isoformat().replace("+00:00", "Z")
        location: dict[str, str] = {"tiploc": tiploc, "stationKey": STATION_BY_TIPLOC[tiploc][0]}
        if at_time:
            location["actualAt"] = at_time.isoformat().replace("+00:00", "Z")
        if eta_time:
            location["forecastAt"] = eta_time.isoformat().replace("+00:00", "Z")
        if location.keys() - {"tiploc", "stationKey"}:
            locations[tiploc] = location
        if tiploc not in route:
            route.append(tiploc)
    if len(route) < 2:
        return False
    services = state.setdefault("services", {})
    prior = services.get(identifier, {})
    service = prior if isinstance(prior, dict) else {}
    merged_locations = dict(service.get("locations", {})) if isinstance(service.get("locations"), dict) else {}
    merged_schedule = dict(service.get("scheduleAt", {})) if isinstance(service.get("scheduleAt"), dict) else {}
    for tiploc, location in locations.items():
        previous = merged_locations.get(tiploc, {})
        if isinstance(previous, dict):
            merged_locations[tiploc] = {**previous, **location}
        else:
            merged_locations[tiploc] = location
    merged_schedule.update(schedule_at)
    merged_route = list(dict.fromkeys(
        (service.get("route", []) if isinstance(service.get("route"), list) else []) + route
    ))
    if len(merged_route) > 1 and all(code in STATION_BY_TIPLOC for code in merged_route):
        ascending = STATION_BY_TIPLOC[merged_route[0]][1] < STATION_BY_TIPLOC[merged_route[1]][1]
        merged_route.sort(key=lambda code: STATION_BY_TIPLOC[code][1], reverse=not ascending)
    stable_display_id = canonical_id if station_suffix else str(
        get(raw, "trainID", "rsid", "serviceID", default="") or identifier
    )
    service.update({
        "rid": identifier,
        "uid": canonical_id if station_suffix else str(get(raw, "serviceID", "rsid", default="") or ""),
        "id": stable_display_id,
        "headcode": str(get(raw, "trainID", "trainId", default="") or "").strip().upper(),
        "serviceDate": now.astimezone(LONDON).date().isoformat(),
        "observedAt": now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "locations": merged_locations,
        "scheduleAt": merged_schedule,
        "route": merged_route,
        "scheduled": bool(merged_schedule),
        "cancelled": str(get(raw, "isCancelled", default="false")).lower() == "true",
        "source": "live_departure_board",
    })
    services[identifier] = service
    return True


def collect_ldb(state: dict[str, Any], now: datetime) -> int:
    """Poll only mapped West Highland stations using the RDM LDBWS API."""
    api_key = os.environ.get("RDM_LDB_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing GitHub Actions secret RDM_LDB_API_KEY")
    base_url = os.environ.get(
        "RDM_LDB_BASE_URL",
        "https://api1.raildata.org.uk/1010-live-departure-board-dep1_2/LDBWS/api/20220120/GetDepBoardWithDetails",
    ).rstrip("/")
    # Earlier refreshes stored one LDB variant per departure station. Fold any
    # already-published variants into the canonical service key before polling.
    saved_services = state.setdefault("services", {})
    station_suffix_pattern = re.compile(
        r"(\d{8})(?:MLAIG|MORAR|ARISAIG|BEASDAL|LCHALRT|GLNFNNN|LCHESDE|LCEILOB|FRTWLM)_*",
        re.IGNORECASE,
    )
    for old_key, old_service in list(saved_services.items()):
        if not isinstance(old_service, dict) or not str(old_key).startswith("ldb:"):
            continue
        match = station_suffix_pattern.fullmatch(str(old_service.get("id") or str(old_key)[4:]))
        if not match:
            continue
        canonical_key = "ldb:" + match.group(1)
        if old_key == canonical_key:
            continue
        target = saved_services.get(canonical_key)
        target = target if isinstance(target, dict) else {}
        merged_service = {**old_service, **target}
        for field in ("locations", "scheduleAt"):
            old_values = old_service.get(field) if isinstance(old_service.get(field), dict) else {}
            new_values = target.get(field) if isinstance(target.get(field), dict) else {}
            merged_service[field] = {**old_values, **new_values}
        route = list(dict.fromkeys(
            (old_service.get("route", []) if isinstance(old_service.get("route"), list) else [])
            + (target.get("route", []) if isinstance(target.get("route"), list) else [])
        ))
        if len(route) > 1 and all(code in STATION_BY_TIPLOC for code in route):
            ascending = STATION_BY_TIPLOC[route[0]][1] < STATION_BY_TIPLOC[route[1]][1]
            route.sort(key=lambda code: STATION_BY_TIPLOC[code][1], reverse=not ascending)
        merged_service.update({"rid": canonical_key, "uid": match.group(1), "id": match.group(1), "route": route})
        saved_services[canonical_key] = merged_service
        saved_services.pop(old_key, None)
    merged = 0
    successful_boards = 0
    failures: list[str] = []
    for crs in LDB_STATION_CRSES:
        query = urllib.parse.urlencode({"numRows": "50", "timeWindow": "120", "timeOffset": "0"})
        request = urllib.request.Request(f"{base_url}/{crs}?{query}", headers={"x-apikey": api_key, "Accept": "application/json", "User-Agent": "JacobiteTrack/1.0"})
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            failures.append(f"{crs}: HTTP {exc.code}")
            continue
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            failures.append(f"{crs}: {type(exc).__name__}")
            continue
        successful_boards += 1
        services = get(payload, "trainServices", "services", default=[])
        if isinstance(services, dict):
            services = get(services, "service", default=[])
        for item in as_list(services):
            if isinstance(item, dict) and _merge_ldb_service(state, item, crs, now):
                merged += 1
    if not successful_boards:
        raise RuntimeError("Live Departure Board could not return any mapped station boards (" + ", ".join(failures) + ")")
    return merged


def line_progress(distance_km: float) -> float:
    """Convert approximate route distance to the app's Mallaig-to-Fort William axis."""
    return 100.0 * (LINE_DISTANCE_KM - distance_km) / LINE_DISTANCE_KM


def is_line_service(service: dict[str, Any]) -> bool:
    """Whether a saved Darwin service calls at a station on the tracked segment."""
    known = set(STATION_BY_TIPLOC)
    locations = service.get("locations", {})
    route = service.get("route", [])
    return (
        isinstance(locations, dict) and any(str(code).upper() in known for code in locations)
    ) or (
        isinstance(route, list) and any(str(code).upper() in known for code in route)
    )


def _key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def get(obj: Any, *names: str, default: Any = None) -> Any:
    if not isinstance(obj, dict):
        return default
    wanted = {_key(name) for name in names}
    for name, value in obj.items():
        if _key(name) in wanted:
            return value
    return default


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def walk_named(value: Any, names: set[str]) -> Iterable[Any]:
    if isinstance(value, dict):
        for name, child in value.items():
            if _key(name) in names:
                yield from as_list(child)
            yield from walk_named(child, names)
    elif isinstance(value, list):
        for child in value:
            yield from walk_named(child, names)


def walk_local_names(value: Any, names: set[str]) -> Iterable[Any]:
    """Walk XML-to-JSON names while ignoring namespace prefixes such as sc:IP."""
    if isinstance(value, dict):
        for name, child in value.items():
            local = str(name).rsplit(":", 1)[-1]
            if _key(local) in names:
                yield child
            yield from walk_local_names(child, names)
    elif isinstance(value, list):
        for child in value:
            yield from walk_local_names(child, names)


def decode_payload(raw: bytes | str | dict[str, Any]) -> dict[str, Any] | None:
    """Unwrap the RDM Kafka JSON/JMS envelope, then parse its Darwin bytes."""
    try:
        outer = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        if isinstance(outer, list):
            return {"_batch": outer}
        if not isinstance(outer, dict):
            return None
        candidates = [outer]
        candidates.extend(v for v in outer.values() if isinstance(v, dict))
        for candidate in candidates:
            encoded = get(candidate, "bytes")
            if isinstance(encoded, str):
                payload = json.loads(encoded)
                if isinstance(payload, list):
                    return {"_batch": payload}
                return payload if isinstance(payload, dict) else None
        return outer
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return None


def parse_datetime(value: Any, service_date: str | None = None, now: datetime | None = None) -> datetime | None:
    """Parse ISO timestamps or Darwin local HHMM/HH:MM times in UK time."""
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"on time", "unknown", "delayed", "no report"}:
        return None
    try:
        iso_text = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(iso_text)
        return parsed.replace(tzinfo=LONDON) if parsed.tzinfo is None else parsed
    except ValueError:
        pass

    match = re.fullmatch(r"(\d{1,2}):?(\d{2})(?::?(\d{2}))?", text)
    if not match:
        return None
    hour, minute, second = int(match.group(1)), int(match.group(2)), int(match.group(3) or 0)
    if hour > 47 or minute > 59 or second > 59:
        return None
    day_offset, hour = divmod(hour, 24)
    if service_date:
        try:
            base = date.fromisoformat(service_date)
        except ValueError:
            base = None
    else:
        base = None
    current = (now or datetime.now(timezone.utc)).astimezone(LONDON)
    base = base or current.date()
    naive = datetime.combine(base + timedelta(days=day_offset), dt_time(hour, minute, second))
    parsed = naive.replace(tzinfo=LONDON)
    if service_date is None and parsed < current - timedelta(hours=12):
        parsed += timedelta(days=1)
    return parsed


def _time_value(group: Any, kind: str) -> Any:
    if isinstance(group, dict):
        return get(group, kind)
    return None


def _location_tiploc(location: Any) -> str:
    value = get(location, "tpl", "tiploc", "locationCode", "location")
    return str(value).strip().upper() if value is not None else ""


def _point_times(location: dict[str, Any], service_date: str | None, now: datetime) -> tuple[list[datetime], list[datetime]]:
    forecasts: list[datetime] = []
    actuals: list[datetime] = []
    for group_name in ("arr", "arrival", "pass", "passing", "dep", "departure"):
        group = get(location, group_name)
        forecast = parse_datetime(_time_value(group, "et"), service_date, now)
        actual = parse_datetime(_time_value(group, "at"), service_date, now)
        if forecast:
            forecasts.append(forecast)
        if actual:
            actuals.append(actual)
    return forecasts, actuals


def _event_timestamp(payload: dict[str, Any], fallback: datetime | None = None) -> datetime | None:
    raw = get(payload, "ts", "timestamp")
    parsed = parse_datetime(raw)
    return parsed or fallback


def _merge_ts(state: dict[str, Any], ts: dict[str, Any], payload_time: datetime) -> None:
    rid = get(ts, "rid", "RID")
    uid = get(ts, "uid", "UID")
    service_date = get(ts, "ssd", "serviceDate", "SSD")
    if not rid and not uid:
        return
    services = state.setdefault("services", {})
    service_date_text = str(service_date) if service_date else None
    identifier = next((key for key, item in services.items()
                       if isinstance(item, dict) and uid and str(item.get("uid", "")).strip() == str(uid).strip()
                       and (not service_date_text or item.get("serviceDate") == service_date_text)), None)
    identifier = identifier or str(rid or uid)
    locations_value = get(ts, "Location", "locations", "location")
    existing = services.get(identifier)
    if existing is None and not any(
        isinstance(location, dict) and _location_tiploc(location) in STATION_BY_TIPLOC
        for location in as_list(locations_value)
    ):
        # The RDM topic is network-wide; don't persist services outside the
        # Mallaig–Fort William segment. Existing matches remain eligible for
        # partial status updates that omit their full calling-point list.
        return
    service = services.setdefault(identifier, {"rid": identifier, "locations": {}, "route": []})
    service["uid"] = str(uid) if uid else service.get("uid")
    service["serviceDate"] = str(service_date) if service_date else service.get("serviceDate")
    train_id = get(ts, "trainId", "trainID", "headcode", "trainIdentifier")
    service["headcode"] = str(train_id or service.get("headcode") or "").strip().upper()
    service["serviceCode"] = str(get(ts, "trainServiceCode", "train_service_code") or service.get("serviceCode") or "").strip()
    service["id"] = str(train_id or service.get("id") or uid or rid)
    service["observedAt"] = payload_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    cancel_reason = get(ts, "cancelReason", "cancellationReason")
    if cancel_reason not in (None, 0, "0", ""):
        service["cancelled"] = True
    elif get(ts, "cancelled", "isCancelled") is True:
        service["cancelled"] = True

    locations = service.setdefault("locations", {})
    order: list[str] = []
    for raw_location in as_list(locations_value):
        if not isinstance(raw_location, dict):
            continue
        tiploc = _location_tiploc(raw_location)
        if tiploc not in STATION_BY_TIPLOC:
            continue
        forecasts, actuals = _point_times(raw_location, service.get("serviceDate"), payload_time)
        saved = locations.setdefault(tiploc, {})
        saved["tiploc"] = tiploc
        station = STATION_BY_TIPLOC.get(tiploc)
        if station:
            saved["stationKey"] = station[0]
        if forecasts:
            nearest = min(forecasts, key=lambda item: abs((item - payload_time).total_seconds()))
            saved["forecastAt"] = nearest.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if actuals:
            latest_actual = max(actuals)
            saved["actualAt"] = latest_actual.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if tiploc not in order:
            order.append(tiploc)
    if order:
        old_order = service.get("route", [])
        # Push Port TS updates often contain only changed locations. Keep the
        # known route and insert newly seen line locations in feed order.
        merged = list(old_order)
        for tiploc in order:
            if tiploc not in merged:
                merged.append(tiploc)
        service["route"] = merged


SCHEDULE_LOCATION_TYPES = {_key(name) for name in ("OR", "OPOR", "PP", "IP", "OPIP", "DT", "OPDT")}


def _merge_schedule(state: dict[str, Any], schedule: dict[str, Any], payload_time: datetime) -> None:
    """Store the map-station timetable points for a Darwin schedule update."""
    rid = get(schedule, "rid", "RID")
    if not rid:
        return
    services = state.setdefault("services", {})
    service_date = get(schedule, "ssd", "serviceDate", "SSD")
    uid = get(schedule, "uid", "UID")
    identifier = next((key for key, item in services.items()
                       if isinstance(item, dict) and uid and str(item.get("uid", "")).strip() == str(uid).strip()
                       and (not service_date or item.get("serviceDate") == str(service_date))), str(rid))
    if get(schedule, "deleted", "isDeleted") in (True, "true", "True", 1, "1"):
        services.pop(identifier, None)
        return
    schedule_times: dict[str, str] = {}
    route: list[str] = []
    for raw_location in walk_local_names(schedule, SCHEDULE_LOCATION_TYPES):
        for location in as_list(raw_location):
            if not isinstance(location, dict):
                continue
            tiploc = _location_tiploc(location)
            if tiploc not in STATION_BY_TIPLOC:
                continue
            # Prefer working times, then fall back to public timetable times.
            # A malformed working time must not hide a valid public time.
            at = None
            for time_name in ("wta", "wtp", "wtd", "pta", "ptd"):
                at = parse_datetime(
                    get(location, time_name),
                    str(service_date) if service_date else None,
                    payload_time,
                )
                if at:
                    break
            if not at:
                continue
            schedule_times[tiploc] = at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if tiploc not in route:
                route.append(tiploc)
    if len(route) < 2:
        return
    service = services.setdefault(identifier, {"rid": identifier, "locations": {}, "route": []})
    service["uid"] = str(get(schedule, "uid", "UID") or service.get("uid") or "")
    service["headcode"] = str(get(schedule, "trainId", "trainID", "signallingId") or service.get("headcode") or "").strip().upper()
    service["serviceCode"] = str(get(schedule, "trainServiceCode", "train_service_code") or service.get("serviceCode") or "").strip()
    service["serviceDate"] = str(service_date or service.get("serviceDate") or "")
    service["id"] = str(get(schedule, "trainId", "trainID") or service.get("id") or service["uid"] or identifier)
    service["observedAt"] = payload_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    service["scheduleAt"] = schedule_times
    service["route"] = route
    service["scheduled"] = True


def _vstp_timestamp(payload: dict[str, Any], fallback: datetime) -> datetime:
    root = get(payload, "VSTPCIFMsgV1")
    raw = get(root, "timestamp")
    try:
        return datetime.fromtimestamp(int(raw) / 1000, timezone.utc) if raw else fallback
    except (TypeError, ValueError, OSError, OverflowError):
        return fallback


def _merge_vstp(state: dict[str, Any], payload: dict[str, Any], fallback: datetime) -> int:
    """Merge one VSTP CIF change, retaining only schedules on the mapped line."""
    batch = get(payload, "_batch")
    if isinstance(batch, list):
        return sum(_merge_vstp(state, item, fallback) for item in batch if isinstance(item, dict))
    root = get(payload, "VSTPCIFMsgV1") or payload
    schedule = get(root, "schedule")
    if not isinstance(schedule, dict):
        return 0
    event_time = _vstp_timestamp(payload, fallback)
    uid = str(get(schedule, "CIF_train_uid", "train_uid", default="") or "").strip()
    service_date = str(get(schedule, "schedule_start_date", default="") or "").strip()
    if not uid or not service_date:
        return 0
    services = state.setdefault("services", {})
    matches = [key for key, item in services.items()
               if isinstance(item, dict) and str(item.get("uid", "")).strip() == uid
               and item.get("serviceDate") == service_date]
    transaction = str(get(schedule, "transaction_type", default="")).strip().lower()
    if transaction == "delete":
        for key in matches:
            services.pop(key, None)
        return 1

    changed = 0
    for segment in as_list(get(schedule, "schedule_segment")):
        if not isinstance(segment, dict):
            continue
        headcode = str(get(segment, "signalling_id", "CIF_headcode", default="") or "").strip().upper()
        points: dict[str, str] = {}
        route: list[str] = []
        for row in as_list(get(segment, "schedule_location")):
            if not isinstance(row, dict):
                continue
            location = get(row, "location")
            tiploc_obj = get(location, "tiploc")
            code = str(get(tiploc_obj, "tiploc_id", default="") or "").strip().upper()
            if code not in STATION_BY_TIPLOC:
                continue
            at = None
            for time_name in ("scheduled_arrival_time", "scheduled_pass_time", "scheduled_departure_time",
                              "public_arrival_time", "public_pass_time", "public_departure_time"):
                at = parse_datetime(get(row, time_name), service_date, event_time)
                if at:
                    break
            if at:
                points[code] = at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                if code not in route:
                    route.append(code)
        if len(route) < 2:
            continue
        identifier = next((key for key in matches if headcode and
                           str(services[key].get("headcode", "")).strip().upper() == headcode), None)
        identifier = identifier or (matches[0] if matches else f"VSTP:{uid}:{service_date}:{headcode}")
        service = services.setdefault(identifier, {"rid": identifier, "uid": uid, "locations": {}, "route": []})
        service["uid"] = uid
        service["serviceDate"] = service_date
        service["headcode"] = headcode or service.get("headcode", "")
        service["serviceCode"] = str(get(schedule, "CIF_train_service_code", "train_service_code", default="") or service.get("serviceCode") or "").strip()
        service["id"] = headcode or service.get("id") or uid
        service["observedAt"] = event_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        service["scheduleAt"] = points
        service["route"] = route
        service["scheduleSource"] = "VSTP"
        service["scheduled"] = True
        changed += 1
    return changed


def merge_movement(payload: dict[str, Any], state: dict[str, Any], fallback: datetime) -> int:
    """Attach a TRUST actual report to a known Darwin/VSTP line service."""
    header, body = get(payload, "header"), get(payload, "body")
    if not isinstance(body, dict) or str(get(header, "msg_type", default="")) != "0003":
        return 0
    stanox = str(get(body, "reporting_stanox", default="") or "").strip()
    if not stanox or stanox == "00000":
        stanox = str(get(body, "original_loc_stanox", "loc_stanox", default="") or "").strip()
    tiploc = STANOX_TO_TIPLOC.get(stanox.zfill(5))
    service_code = str(get(body, "train_service_code", default="") or "").strip()
    raw_at = get(body, "actual_timestamp")
    try:
        actual_at = datetime.fromtimestamp(int(raw_at) / 1000, timezone.utc) if raw_at else fallback
    except (TypeError, ValueError, OSError, OverflowError):
        actual_at = fallback
    # A documented TRUST timestamp issue can add an hour during British
    # Summer Time. Correct only the characteristic future offset.
    actual_local = actual_at.astimezone(LONDON)
    delta = (actual_at - fallback).total_seconds()
    if actual_local.dst() and 1800 < delta < 5400:
        actual_at -= timedelta(hours=1)
    if not tiploc or not service_code or abs((fallback - actual_at).total_seconds()) > 6 * 3600:
        return 0
    for service in state.setdefault("services", {}).values():
        if not isinstance(service, dict) or str(service.get("serviceCode", "")).strip() != service_code:
            continue
        if not is_line_service(service):
            continue
        reported_date = actual_at.astimezone(LONDON).date().isoformat()
        service_date = str(service.get("serviceDate", ""))
        if service_date and service_date not in {
            reported_date,
            (actual_at.astimezone(LONDON).date() - timedelta(days=1)).isoformat(),
            (actual_at.astimezone(LONDON).date() + timedelta(days=1)).isoformat(),
        }:
            continue
        locations = service.setdefault("locations", {})
        location = locations.setdefault(tiploc, {"tiploc": tiploc, "stationKey": STATION_BY_TIPLOC[tiploc][0]})
        old = _to_utc(location.get("actualAt"))
        if old and old > actual_at:
            return 0
        location["actualAt"] = actual_at.isoformat().replace("+00:00", "Z")
        service["observedAt"] = fallback.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return 1
    return 0


def merge_train_describer(payload: dict[str, Any], state: dict[str, Any], fallback: datetime) -> int:
    """Keep only TD berth updates for trains already identified on this line.

    Berths are not converted to map positions: that requires the signalling
    berth-stepping/geometry reference, which is not included with the feed.
    """
    batch = get(payload, "_batch")
    if isinstance(batch, list):
        return sum(merge_train_describer(item, state, fallback) for item in batch if isinstance(item, dict))
    changed = 0
    for value in payload.values():
        if not isinstance(value, dict):
            continue
        descr = str(get(value, "descr", default="") or "").strip().upper()
        msg_type = str(get(value, "msg_type", default="") or "").upper()
        if msg_type not in {"CA", "CC"} or not descr:
            continue
        raw_time = get(value, "time")
        try:
            seen = datetime.fromtimestamp(int(raw_time) / 1000, timezone.utc) if raw_time else fallback
        except (TypeError, ValueError, OSError, OverflowError):
            seen = fallback
        if abs((fallback - seen).total_seconds()) > 10 * 60:
            continue
        for service in state.setdefault("services", {}).values():
            if not isinstance(service, dict) or not is_line_service(service):
                continue
            if str(service.get("headcode", service.get("id", ""))).strip().upper() != descr:
                continue
            service["tdLastSeen"] = seen.isoformat().replace("+00:00", "Z")
            service["tdArea"] = str(get(value, "area_id", default="") or "")
            service["tdBerth"] = str(get(value, "to", "from", default="") or "")
            changed += 1
            break
    return changed


def process_message(raw: bytes | str | dict[str, Any], state: dict[str, Any], fallback_time: datetime | None = None) -> int:
    payload = decode_payload(raw)
    if not payload:
        return 0
    batch = get(payload, "_batch")
    if isinstance(batch, list):
        return sum(process_message(item, state, fallback_time) for item in batch)
    event_time = _event_timestamp(payload, fallback_time)
    if not event_time:
        return 0
    count = 0
    for schedule in walk_named(payload, {"schedule"}):
        if isinstance(schedule, dict) and get(schedule, "rid", "RID"):
            _merge_schedule(state, schedule, event_time)
            count += 1
    for deactivated in walk_named(payload, {"deactivatedSchedule"}):
        if isinstance(deactivated, dict):
            rid = get(deactivated, "rid", "RID")
            if rid:
                state.setdefault("services", {}).pop(str(rid), None)
    for ts in walk_named(payload, {"ts"}):
        if isinstance(ts, dict) and get(ts, "rid", "RID", "uid", "UID"):
            _merge_ts(state, ts, event_time)
            count += 1
    return count


def _to_utc(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def build_feed(state: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    trains: list[dict[str, Any]] = []
    for service in state.get("services", {}).values():
        observed = _to_utc(service.get("observedAt"))
        schedule_points = [
            (at, STATION_BY_TIPLOC[code][1], STATION_BY_TIPLOC[code][0])
            for code, raw_at in service.get("scheduleAt", {}).items()
            if code in STATION_BY_TIPLOC and (at := _to_utc(raw_at)) is not None
        ]
        if schedule_points:
            service_start = min(point[0] for point in schedule_points)
            service_end = max(point[0] for point in schedule_points)
            if service_start > now + SCHEDULE_HORIZON or service_end < now - timedelta(minutes=20):
                continue
        elif not observed or (now - observed).total_seconds() > FRESH_SECONDS or (observed - now).total_seconds() > 60:
            continue
        viewpoints: dict[str, dict[str, Any]] = {}
        actual_points: list[tuple[datetime, float, str]] = []
        forecast_points: list[tuple[datetime, float, str]] = []
        direction_values: list[float] = []
        route = service.get("route", [])
        line_sequence = [STATION_BY_TIPLOC[code][1] for code in route if code in STATION_BY_TIPLOC]
        if len(line_sequence) >= 2:
            direction_values = [b - a for a, b in zip(line_sequence, line_sequence[1:]) if a != b]
        direction = "out" if direction_values and direction_values[-1] > 0 else "back" if direction_values else None
        for tiploc, location in service.get("locations", {}).items():
            station = STATION_BY_TIPLOC.get(tiploc)
            if not station:
                continue
            eta_at = _to_utc(location.get("forecastAt"))
            actual_at = _to_utc(location.get("actualAt"))
            if actual_at and actual_at <= now + timedelta(seconds=60):
                actual_points.append((actual_at, station[1], station[0]))
            if eta_at:
                forecast_points.append((eta_at, station[1], station[0]))
            if station[0] in VIEWPOINT_BY_KEY and eta_at:
                minutes = (eta_at - now).total_seconds() / 60
                if -1 <= minutes <= SCHEDULE_HORIZON.total_seconds() / 60:
                    key = VIEWPOINT_BY_KEY[station[0]]
                    old = viewpoints.get(key)
                    if old is None or minutes < old["etaMinutes"]:
                        viewpoints[key] = {"etaMinutes": max(0.0, round(minutes, 2))}
            if station[0] in VIEWPOINT_BY_KEY and actual_at and abs((now - actual_at).total_seconds()) < 180:
                key = VIEWPOINT_BY_KEY[station[0]]
                viewpoints.setdefault(key, {"etaMinutes": 0.0})

        # Schedule data fills the roster and offers a lower-confidence
        # position estimate when a live actual/forecast anchor is unavailable.
        for at, progress_index, station_key in schedule_points:
            if station_key in VIEWPOINT_BY_KEY and at >= now:
                key = VIEWPOINT_BY_KEY[station_key]
                old = viewpoints.get(key)
                minutes = (at - now).total_seconds() / 60
                if 0 <= minutes <= SCHEDULE_HORIZON.total_seconds() / 60 and (old is None or minutes < old["etaMinutes"]):
                    viewpoints[key] = {"etaMinutes": round(minutes, 2)}

        # Estimate the viaduct passage between the nearest timed stations.
        # Use a recent actual point as the lower anchor, then prefer future
        # Darwin forecasts over the working timetable at each station.
        if direction is None:
            direction_reports = sorted(actual_points, key=lambda item: item[0])
            if len(direction_reports) >= 2 and direction_reports[-1][1] != direction_reports[-2][1]:
                direction = "out" if direction_reports[-1][1] > direction_reports[-2][1] else "back"
            else:
                latest_actual = max(actual_points, default=None, key=lambda item: item[0])
                future_reports = [point for point in forecast_points if point[0] > now]
                if latest_actual and future_reports:
                    next_report = min(future_reports, key=lambda item: item[0])
                    if next_report[1] != latest_actual[1]:
                        direction = "out" if next_report[1] > latest_actual[1] else "back"
        if direction in {"out", "back"}:
            candidates: dict[float, tuple[datetime, str, str]] = {}
            for at, distance, station_key in actual_points:
                if timedelta(0) <= now - at <= timedelta(hours=12):
                    candidates[distance] = (at, station_key, "actual")
            for at, distance, station_key in forecast_points:
                if at >= now - timedelta(minutes=2):
                    old = candidates.get(distance)
                    if old is None or old[0] < now or (at >= now and at < old[0]):
                        candidates[distance] = (at, station_key, "forecast")
            for at, distance, station_key in schedule_points:
                old = candidates.get(distance)
                if at >= now and (old is None or old[0] < now):
                    candidates[distance] = (at, station_key, "schedule")

            timed_by_distance = sorted(
                ((distance, *point) for distance, point in candidates.items()),
                key=lambda point: point[0],
            )
            viaduct_eta: datetime | None = None
            viaduct_basis: str | None = None
            for left, right in zip(timed_by_distance, timed_by_distance[1:]):
                # Inbound trains travel from greater to smaller distance.
                a, b = (right, left) if direction == "back" else (left, right)
                if not (min(a[0], b[0]) <= VIADUCT_DISTANCE_KM <= max(a[0], b[0])):
                    continue
                if b[1] <= a[1]:
                    continue
                fraction = (VIADUCT_DISTANCE_KM - a[0]) / (b[0] - a[0])
                candidate_eta = a[1] + (b[1] - a[1]) * fraction
                if candidate_eta < now or candidate_eta > now + SCHEDULE_HORIZON:
                    continue
                if viaduct_eta is None or candidate_eta < viaduct_eta:
                    viaduct_eta = candidate_eta
                    viaduct_basis = "scheduled" if a[3] == b[3] == "schedule" else "live_or_scheduled"
            if viaduct_eta:
                minutes = (viaduct_eta - now).total_seconds() / 60
                # This viewpoint is the viaduct crossing itself. For Mallaig-
                # bound trains the crossing is after Glenfinnan station, so
                # always replace any station-call ETA with the interpolated
                # crossing time in either direction.
                viewpoints["glenfinnan"] = {
                    "etaMinutes": round(minutes, 2),
                    "quality": "estimated_between_station_times",
                    "basis": viaduct_basis or "station_times",
                }

        # If route ordering is incomplete, infer direction from time-ordered
        # station reports and then from the next Darwin forecast.
        if direction is None:
            timed = sorted(actual_points, key=lambda item: item[0])
            if len(timed) >= 2 and timed[-1][1] != timed[-2][1]:
                direction = "out" if timed[-1][1] > timed[-2][1] else "back"
            else:
                latest_actual = max(actual_points, default=None, key=lambda item: item[0])
                future = [p for p in forecast_points if p[0] > now]
                if latest_actual and future:
                    next_point = min(future, key=lambda item: item[0])
                    if next_point[1] != latest_actual[1]:
                        direction = "out" if next_point[1] > latest_actual[1] else "back"

        lower_actuals = [p for p in actual_points if p[0] <= now]
        lower_forecasts = [p for p in forecast_points if p[0] <= now]
        lower = max(lower_actuals, default=None, key=lambda item: item[0])
        basis = "actual_report"
        if lower is None:
            lower = max(lower_forecasts, default=None, key=lambda item: item[0])
            basis = "darwin_forecasts"
        if lower is None:
            lower = max((p for p in schedule_points if p[0] <= now), default=None, key=lambda item: item[0])
            basis = "scheduled_timetable"
        if lower is None:
            # A service that has not departed yet still belongs on the line
            # roster. Hold its estimate at its first scheduled station until
            # that departure time instead of hiding it from the map.
            lower = min(schedule_points, default=None, key=lambda item: item[0])
            basis = "scheduled_timetable"

        upper = None
        if lower is not None and direction:
            ahead = [
                p for p in forecast_points + schedule_points
                if p[0] > now and (p[1] - lower[1]) * (1 if direction == "out" else -1) > 0
            ]
            upper = min(ahead, default=None, key=lambda item: item[0])

        progress = None
        position_label = None
        position = None
        if lower is not None:
            _, from_index, from_key = lower
            from_progress = line_progress(from_index)
            to_progress = from_progress
            to_at = None
            to_key = None
            if upper and upper[0] > lower[0]:
                to_at, to_index, to_key = upper
                to_progress = line_progress(to_index)
                fraction = max(0.0, min(1.0, (now - lower[0]).total_seconds() / (to_at - lower[0]).total_seconds()))
                progress = from_progress + (to_progress - from_progress) * fraction
                quality = "scheduled_estimate" if basis == "scheduled_timetable" else "darwin_estimate" if basis == "darwin_forecasts" else "actual_plus_forecast_estimate"
                position_label = "Estimated " + STATION_NAME_BY_KEY.get(from_key, from_key) + "–" + STATION_NAME_BY_KEY.get(to_key, to_key)
            else:
                progress = from_progress
                quality = "last_actual_report" if basis == "actual_report" else "forecast_station_only" if basis == "darwin_forecasts" else "scheduled_station_only"
                position_label = ("Last actual report: " if basis == "actual_report" else "Forecast point: ") + STATION_NAME_BY_KEY.get(from_key, from_key)
            position = {
                "fromProgress": from_progress,
                "toProgress": to_progress,
                "fromAt": lower[0].isoformat().replace("+00:00", "Z"),
                "toAt": to_at.isoformat().replace("+00:00", "Z") if to_at else None,
                "basis": quality,
                "fromStation": STATION_NAME_BY_KEY.get(from_key, from_key),
                "toStation": STATION_NAME_BY_KEY.get(to_key, to_key) if to_key else None,
            }
        else:
            quality = "no_position_anchor"

        trains.append({
            "id": str(service.get("id") or service.get("uid") or service.get("rid")),
            "observedAt": service["observedAt"],
            "direction": direction or "unknown",
            "progress": progress,
            "positionLabel": position_label,
            "positionQuality": quality,
            "position": position,
            "scheduled": bool(schedule_points),
            "locationVerified": False,
            "lat": None,
            "lon": None,
            "cancelled": bool(service.get("cancelled", False)),
            "viewpoints": viewpoints,
            "quality": "Darwin actual/forecast report or scheduled timetable; interpolated line position is an estimate, not GPS",
        })
    trains.sort(key=lambda item: min((v["etaMinutes"] for v in item["viewpoints"].values()), default=9999))
    trains = trains[:100]
    return {
        "updated": now.isoformat().replace("+00:00", "Z"),
        "source": "Darwin/VSTP and TRUST via Rail Data Marketplace",
        "quality": "Darwin/VSTP station times and TRUST actual reports; interpolated positions are estimates, not GPS.",
        "status": "ok" if trains else "no_recent_reports",
        "sources": state.get("sources", {}),
        "trains": trains,
    }


def load_json(path: Path, fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        json.dump(value, tmp, separators=(",", ":"), ensure_ascii=False)
        tmp.write("\n")
        temp_name = tmp.name
    os.replace(temp_name, path)


def main() -> int:
    mode = os.environ.get("RDM_MODE", "darwin").strip().lower()
    if mode == "ldb":
        now = datetime.now(timezone.utc)
        state = load_json(STATE_PATH, {"services": {}})
        state.setdefault("services", {})
        try:
            matched = collect_ldb(state, now)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        state.setdefault("sources", {})["ldb"] = {"updated": now.isoformat().replace("+00:00", "Z"), "servicesMatched": matched}
        state["savedAt"] = now.isoformat().replace("+00:00", "Z")
        write_json(STATE_PATH, state)
        feed = build_feed(state, now)
        write_json(FEED_PATH, feed)
        print(f"Published {len(feed['trains'])} West Highland service estimate(s); LDB matched {matched} station-board records.")
        return 0
    stream_secrets = {
        "darwin": ("RDM_BOOTSTRAP_SERVERS", "RDM_CONSUMER_USERNAME", "RDM_CONSUMER_PASSWORD", "RDM_GROUP_ID", "RDM_TOPIC"),
        "movements": ("RDM_NWR_BOOTSTRAP_SERVERS", "RDM_NWR_CONSUMER_USERNAME", "RDM_NWR_CONSUMER_PASSWORD", "RDM_MOVEMENTS_GROUP_ID", "RDM_MOVEMENTS_TOPIC"),
        "vstp": ("RDM_NWR_BOOTSTRAP_SERVERS", "RDM_NWR_CONSUMER_USERNAME", "RDM_NWR_CONSUMER_PASSWORD", "RDM_VSTP_GROUP_ID", "RDM_VSTP_TOPIC"),
        "td": ("RDM_NWR_BOOTSTRAP_SERVERS", "RDM_NWR_CONSUMER_USERNAME", "RDM_NWR_CONSUMER_PASSWORD", "RDM_TD_GROUP_ID", "RDM_TD_TOPIC"),
    }
    if mode not in stream_secrets:
        print("RDM_MODE must be ldb, darwin, movements, vstp, or td.", file=sys.stderr)
        return 2
    required = stream_secrets[mode]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        print("Missing GitHub Actions secret(s): " + ", ".join(missing), file=sys.stderr)
        return 2
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        print("Install the confluent-kafka dependency before running this consumer.", file=sys.stderr)
        return 2

    state = load_json(STATE_PATH, {"services": {}})
    all_services = state.get("services", {})
    if not isinstance(all_services, dict):
        all_services = {}
    before_filter = len(all_services)
    state["services"] = {
        identifier: service
        for identifier, service in all_services.items()
        if isinstance(service, dict) and is_line_service(service)
    }
    for service in state["services"].values():
        if not isinstance(service.get("locations"), dict):
            service["locations"] = {}
        if not isinstance(service.get("route"), list):
            service["route"] = []
        if not isinstance(service.get("scheduleAt"), dict):
            service["scheduleAt"] = {}
        service["locations"] = {
            code: value for code, value in service.get("locations", {}).items()
            if code in STATION_BY_TIPLOC
        }
        service["route"] = [code for code in service.get("route", []) if code in STATION_BY_TIPLOC]
        service["scheduleAt"] = {
            code: value for code, value in service.get("scheduleAt", {}).items()
            if code in STATION_BY_TIPLOC
        }
    removed_unrelated = before_filter - len(state["services"])
    bootstrap, username, password, group_id, topic = (os.environ[name] for name in required)
    config = {
        "bootstrap.servers": bootstrap,
        "sasl.username": username,
        "sasl.password": password,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "socket.timeout.ms": 10000,
    }
    consumer = Consumer(config)
    processed = 0
    deadline = time.monotonic() + int(os.environ.get("RDM_POLL_SECONDS", "75"))
    try:
        consumer.subscribe([topic])
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print("Kafka consume error: " + message.error().str(), file=sys.stderr)
                return 1
            timestamp = None
            try:
                kind, milliseconds = message.timestamp()
                if milliseconds and kind != -1:
                    timestamp = datetime.fromtimestamp(milliseconds / 1000, timezone.utc)
            except (AttributeError, OSError, ValueError):
                pass
            raw = message.value()
            if mode == "darwin":
                processed += process_message(raw, state, timestamp)
            else:
                payload = decode_payload(raw)
                if not payload:
                    continue
                effective_time = timestamp or datetime.now(timezone.utc)
                if mode == "vstp":
                    processed += _merge_vstp(state, payload, effective_time)
                elif mode == "movements":
                    batch = get(payload, "_batch")
                    items = batch if isinstance(batch, list) else [payload]
                    processed += sum(merge_movement(item, state, effective_time) for item in items if isinstance(item, dict))
                elif mode == "td":
                    processed += merge_train_describer(payload, state, effective_time)
    except Exception as exc:
        print("Kafka feed collection failed: " + str(exc), file=sys.stderr)
        return 1
    finally:
        consumer.close()

    now = datetime.now(timezone.utc)
    state.setdefault("sources", {})[mode] = {"updated": now.isoformat().replace("+00:00", "Z"), "messages": processed}
    for rid, item in list(state.get("services", {}).items()):
        observed = _to_utc(item.get("observedAt"))
        schedule_points = [_to_utc(value) for value in item.get("scheduleAt", {}).values()]
        schedule_points = [value for value in schedule_points if value]
        if schedule_points:
            if max(schedule_points) < now - timedelta(minutes=20):
                del state["services"][rid]
        elif not observed or (now - observed).total_seconds() > 6 * 3600:
            del state["services"][rid]
    state["savedAt"] = now.isoformat().replace("+00:00", "Z")
    write_json(STATE_PATH, state)
    feed = build_feed(state, now)
    write_json(FEED_PATH, feed)
    print(f"Published {len(feed['trains'])} fresh West Highland service(s); {mode} reports matched {processed} time(s).")
    print(f"Kept {len(state['services'])} West Highland segment service(s); discarded {removed_unrelated} unrelated saved service(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
