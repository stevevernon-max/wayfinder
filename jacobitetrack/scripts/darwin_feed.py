#!/usr/bin/env python3
"""Consume recent Darwin Push Port JSON messages and publish a small feed.

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
    "MLAIG": ("mallaig", 0),
    "MORAR": ("morar", 1),
    "ARISAIG": ("arisaig", 2),
    "BEASDAL": ("beasdale", 2.5),
    "LCHALRT": ("lochailort", 3),
    "GLNFNNN": ("glenfinnan", 4),
    "LCHESDE": ("locheilside", 4.5),
    "LCEILOB": ("lochEilOutwardBound", 4.65),
    "CORPACH": ("corpach", 4.8),
    "BANAVIE": ("banavie", 4.9),
    "FRTWLM": ("fortWilliam", 5),
    # Accept descriptive variants in case another Darwin version emits them.
    "MALLAIG": ("mallaig", 0),
    "LOCHAILORT": ("lochailort", 3),
    "GLENFINNAN": ("glenfinnan", 4),
    "FTWILLIAM": ("fortWilliam", 5),
    "FORTWILLIAM": ("fortWilliam", 5),
}
STATION_NAME_BY_KEY = {
    "mallaig": "Mallaig",
    "morar": "Morar",
    "arisaig": "Arisaig",
    "beasdale": "Beasdale",
    "lochailort": "Lochailort",
    "glenfinnan": "Glenfinnan",
    "locheilside": "Locheilside",
    "lochEilOutwardBound": "Loch Eil Outward Bound",
    "corpach": "Corpach",
    "banavie": "Banavie",
    "fortWilliam": "Fort William",
}
VIEWPOINT_BY_KEY = {
    "morar": "morar",
    "arisaig": "arisaig",
    "lochailort": "lochailort",
    "glenfinnan": "glenfinnan",
}
LINE_TIPLOCS = set(STATION_BY_TIPLOC)
FRESH_SECONDS = 600


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


def decode_payload(raw: bytes | str | dict[str, Any]) -> dict[str, Any] | None:
    """Unwrap the RDM Kafka JSON/JMS envelope, then parse its Darwin bytes."""
    try:
        outer = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        if not isinstance(outer, dict):
            return None
        candidates = [outer]
        candidates.extend(v for v in outer.values() if isinstance(v, dict))
        for candidate in candidates:
            encoded = get(candidate, "bytes")
            if isinstance(encoded, str):
                payload = json.loads(encoded)
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
    identifier = str(rid or uid)
    services = state.setdefault("services", {})
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
        if not tiploc:
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


def process_message(raw: bytes | str | dict[str, Any], state: dict[str, Any], fallback_time: datetime | None = None) -> int:
    payload = decode_payload(raw)
    if not payload:
        return 0
    event_time = _event_timestamp(payload, fallback_time)
    if not event_time:
        return 0
    count = 0
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
        if not observed or (now - observed).total_seconds() > FRESH_SECONDS or (observed - now).total_seconds() > 60:
            continue
        viewpoints: dict[str, dict[str, float]] = {}
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
                if -1 <= minutes <= 360:
                    key = VIEWPOINT_BY_KEY[station[0]]
                    old = viewpoints.get(key)
                    if old is None or minutes < old["etaMinutes"]:
                        viewpoints[key] = {"etaMinutes": max(0.0, round(minutes, 2))}
            if station[0] in VIEWPOINT_BY_KEY and actual_at and abs((now - actual_at).total_seconds()) < 180:
                key = VIEWPOINT_BY_KEY[station[0]]
                viewpoints.setdefault(key, {"etaMinutes": 0.0})

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

        upper = None
        if lower is not None and direction:
            ahead = [
                p for p in forecast_points
                if p[0] > now and (p[1] - lower[1]) * (1 if direction == "out" else -1) > 0
            ]
            upper = min(ahead, default=None, key=lambda item: item[0])

        progress = None
        position_label = None
        position = None
        if lower is not None:
            _, from_index, from_key = lower
            from_progress = float((5 - from_index) * 20)
            to_progress = from_progress
            to_at = None
            to_key = None
            if upper and upper[0] > lower[0]:
                to_at, to_index, to_key = upper
                to_progress = float((5 - to_index) * 20)
                fraction = max(0.0, min(1.0, (now - lower[0]).total_seconds() / (to_at - lower[0]).total_seconds()))
                progress = from_progress + (to_progress - from_progress) * fraction
                quality = "darwin_estimate" if basis == "darwin_forecasts" else "actual_plus_forecast_estimate"
                position_label = "Estimated " + STATION_NAME_BY_KEY.get(from_key, from_key) + "–" + STATION_NAME_BY_KEY.get(to_key, to_key)
            else:
                progress = from_progress
                quality = "last_actual_report" if basis == "actual_report" else "forecast_station_only"
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
            "locationVerified": False,
            "lat": None,
            "lon": None,
            "cancelled": bool(service.get("cancelled", False)),
            "viewpoints": viewpoints,
            "quality": "Darwin reported times; interpolated line position is an estimate, not GPS",
        })
    trains.sort(key=lambda item: min((v["etaMinutes"] for v in item["viewpoints"].values()), default=9999))
    trains = trains[:25]
    return {
        "updated": now.isoformat().replace("+00:00", "Z"),
        "source": "Darwin via Rail Data Marketplace",
        "quality": "Darwin actual and forecast times; interpolated train positions are estimates, not GPS.",
        "status": "ok" if trains else "no_recent_reports",
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
    required = ("RDM_BOOTSTRAP_SERVERS", "RDM_CONSUMER_USERNAME", "RDM_CONSUMER_PASSWORD", "RDM_GROUP_ID", "RDM_TOPIC")
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
    removed_unrelated = before_filter - len(state["services"])
    config = {
        "bootstrap.servers": os.environ["RDM_BOOTSTRAP_SERVERS"],
        "sasl.username": os.environ["RDM_CONSUMER_USERNAME"],
        "sasl.password": os.environ["RDM_CONSUMER_PASSWORD"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "group.id": os.environ["RDM_GROUP_ID"],
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "socket.timeout.ms": 10000,
    }
    consumer = Consumer(config)
    processed = 0
    deadline = time.monotonic() + int(os.environ.get("RDM_POLL_SECONDS", "75"))
    try:
        consumer.subscribe([os.environ["RDM_TOPIC"]])
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
            processed += process_message(message.value(), state, timestamp)
    except Exception as exc:
        print("Kafka feed collection failed: " + str(exc), file=sys.stderr)
        return 1
    finally:
        consumer.close()

    now = datetime.now(timezone.utc)
    for rid, item in list(state.get("services", {}).items()):
        observed = _to_utc(item.get("observedAt"))
        if not observed or (now - observed).total_seconds() > 6 * 3600:
            del state["services"][rid]
    state["savedAt"] = now.isoformat().replace("+00:00", "Z")
    write_json(STATE_PATH, state)
    feed = build_feed(state, now)
    write_json(FEED_PATH, feed)
    print(f"Published {len(feed['trains'])} fresh Darwin service report(s); processed {processed} status update(s).")
    print(f"Kept {len(state['services'])} West Highland segment service(s); discarded {removed_unrelated} unrelated saved service(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
