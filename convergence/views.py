import json
from collections import defaultdict
from decimal import Decimal

from django.contrib.auth.decorators import login_required, permission_required
from django.db.models.functions import Trim
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from convergence.models import ConvergenceBusToRail, ConvergenceRailToBus, OverrideConv, RawBusData

# region helpers
def _format_percentage(value):
    if value is None:
        return ""
    if isinstance(value, Decimal):
        value = float(value)
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return ""
        return text if text.endswith("%") else f"{text}%"

def _to_int_or_none(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None
# endregion helpers

def _extract_hhmm(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if "T" in text:
        text = text.split("T")[-1]
    if " " in text:
        text = text.split(" ")[-1]
    if "+" in text:
        text = text.split("+")[0]
    if "." in text:
        text = text.split(".")[0]
    parts = text.split(":")
    if len(parts) < 2:
        return text
    try:
        hh = int(float(parts[0])) % 24
        mm = int(float(parts[1]))
    except (TypeError, ValueError):
        return text
    return f"{hh:02d}:{mm:02d}"


def _row_year_month(row):
    try:
        return f"{int(row.year):04d}-{int(row.month):02d}"
    except (TypeError, ValueError):
        return f"{row.year}-{row.month}"

# region organizing the data from DB
COL_STATION = "שם תחנת הרכבת"
COL_YEAR = "שנה"
COL_MONTH = "חודש"
COL_WEEK = "תקופת שבוע"
COL_RAIL_DIR = "כיוון נסיעת הרכבת"
COL_TRAIN_ID = "מספר הרכבת"
COL_PERC = "אחוז הנסיעות שעמדו בזמנים"
COL_PERC_BY_MAKAT_FOR_TREND = "אחוז הנסיעות שעמדו בזמנים ברמת מקט (עבור הטרנד דאטה)"
COL_PERC_BY_MAKAT = "אחוז הנסיעות שעמדו בזמנים ברמת מקט"
COL_PERC_BY_TRAIN = "אחוז הנסיעות שעמדו בזמנים ברמת נסיעת הרכבת"
COL_PERC_BY_TRAIN_STATION = "אחוז הנסיעות שעמדו בזמנים ברמת תחנת רכבת"
COL_N = "מספר תצפיות"
COL_N_POSITIVE_FLAGGED = "מספר הנסיעות שעמדו בזמנים"
COL_SIGNAGE = "שילוט"
COL_GOLD_TRAIN = "רכבת זהב"
COL_EXPRESS_TRAIN = "סוג רכבת"
COL_BUS_ON_TIME = "האם האוטובוס מגיע בזמן"
COL_LICENSED_TRAIN_ARRIVAL = "שעת הגעת הרכבת לתחנה (רישוי)"
COL_BUS_ARRIVAL_TO_STATION = "שעת הגעה לתחנה (בממוצע)"
RAIL_DIRECTION_TO_TLV = "לכיוון תל אביב"
ON_TIME_GAP_MINUTES_LOWER_LIMIT = 8
ON_TIME_GAP_MINUTES_UPPER_LIMIT = 15

COL_TRAIN_STATION_CODE = "__train_station_code"
COL_FROM_TRAIN_NUMBER = "__from_train_number"
COL_FROM_TRAIN_ARRIVAL = "__from_train_rishui_train_arrival_time"
COL_LINK_DIRECTION = "__link_direction"

def _serialize_bus_to_rail(row):
    return {
        COL_YEAR: row.year,
        COL_MONTH: row.month,
        COL_WEEK: row.week_period,
        COL_STATION: row.train_station_name,
        COL_RAIL_DIR: row.rail_direction,
        COL_TRAIN_ID: row.train_number,
        COL_SIGNAGE: row.signage,
        COL_GOLD_TRAIN: row.is_gold_train,
        COL_EXPRESS_TRAIN: row.express_train,
        COL_BUS_ON_TIME: row.is_bus_on_time,
        COL_LICENSED_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_TRAIN_STATION_CODE: row.train_station_code,
        COL_FROM_TRAIN_NUMBER: row.train_number,
        COL_FROM_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_LINK_DIRECTION: "bus_to_rail",
        "זמן נסיעת הרכבת לתחנת רכבת השלום (דקות)": row.duration_from_current_station_to_hashalom,
        "מספר עולים": row.train_ascending_amount,
        "מפעיל": row.operator,
        'מק"ט': row.makat,
        "כיוון": row.direction,
        "חלופה": row.alternative,
        "שעת יציאה מתחנת המוצא": row.departure_time,
        "ממוצע נוסעים לנסיעה": row.avg_passengers_per_trip,
        "שעת הגעה לתחנה (בממוצע)": row.arrival_time_to_station,
        "סטיית תקן משעת ההגעה לתחנה": row.arrival_time_window,
        "הפרש בדקות (מאוטובוס לרכבת)": row.minutes_gap_bus_to_rail,
        "המלצה (דקות)": row.recommended_minutes,
        COL_N: row.observations_count,
        COL_N_POSITIVE_FLAGGED: row.on_time_count,
    }


def _serialize_rail_to_bus(row):
    return {
        COL_YEAR: row.year,
        COL_MONTH: row.month,
        COL_WEEK: row.week_period,
        COL_STATION: row.train_station_name,
        COL_RAIL_DIR: row.rail_direction,
        COL_TRAIN_ID: row.train_number,
        COL_SIGNAGE: row.signage,
        COL_GOLD_TRAIN: row.is_gold_train,
        COL_EXPRESS_TRAIN: row.express_train,
        COL_BUS_ON_TIME: row.is_bus_on_time,
        COL_LICENSED_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_TRAIN_STATION_CODE: row.train_station_code,
        COL_FROM_TRAIN_NUMBER: row.train_number,
        COL_FROM_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_LINK_DIRECTION: "rail_to_bus",
        "זמן נסיעת הרכבת מתחנת רכבת השלום (דקות)": row.duration_from_hashalom_to_current_station,
        "מספר יורדים": row.train_descending_amount,
        "מפעיל": row.operator,
        'מק"ט': row.makat,
        "כיוון": row.direction,
        "חלופה": row.alternative,
        "שעת יציאה מתחנת המוצא": row.departure_time,
        "ממוצע נוסעים לנסיעה": row.avg_passengers_per_trip,
        "הפרש בדקות (מרכבת לאוטובוס)": row.minutes_gap_rail_to_bus,
        "המלצה (דקות)": row.recommended_minutes,
    }


def _serialize_bus_to_rail_trend(row): #NOTE - for trend by station level i will use a column from "_serialize_bus_to_rail"
    return {
        COL_RAIL_DIR: row.rail_direction,
        COL_YEAR: row.year,
        COL_MONTH: row.month,
        COL_WEEK: row.week_period,
        COL_STATION: row.train_station_name,
        COL_TRAIN_ID: row.train_number,
        COL_LICENSED_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_SIGNAGE: row.signage,
        COL_FROM_TRAIN_NUMBER: row.train_number,
        COL_FROM_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_LINK_DIRECTION: "bus_to_rail",
        'מק"ט': row.makat,
        "כיוון": row.direction,
        "חלופה": row.alternative,
        "שעת יציאה מתחנת המוצא": row.departure_time,
        COL_BUS_ARRIVAL_TO_STATION: row.arrival_time_to_station,
    }



# endregion organizing the data from DB

# region override
def _row_override_key(row):
    return (
        str(row.get(COL_STATION) or "").strip(),
        str(row.get(COL_WEEK) or "").strip(),
        str(row.get(COL_LINK_DIRECTION) or "").strip(),
        _to_int_or_none(row.get('מק"ט')),
        _to_int_or_none(row.get("כיוון")),
        str(row.get("חלופה") or "").strip(),
        str(row.get("שעת יציאה מתחנת המוצא") or "").strip(),
        _to_int_or_none(row.get(COL_FROM_TRAIN_NUMBER)),
        str(row.get(COL_FROM_TRAIN_ARRIVAL) or "").strip(),
    )


def _build_override_lookup(effective_month):
    out = {}
    if not effective_month:
        return out

    qs = OverrideConv.objects.filter(effective_month__lte=effective_month).order_by("changed_at")

    for ov in qs:
        key = (
            str(ov.station_name or "").strip(),
            str(ov.week_period or "").strip(),
            str(ov.link_direction or "").strip(),
            ov.makat,
            ov.direction,
            str(ov.alternative or "").strip(),
            str(ov.departure_time or "").strip(),
            ov.from_train_number,
            str(ov.from_train_rishui_train_arrival_time or "").strip(),
        )
        out[key] = ov
    return out


def _apply_overrides_to_rows(rows, override_lookup):
    for row in rows:
        key = _row_override_key(row)
        ov = override_lookup.get(key)
        if ov is None:
            continue
        if ov.to_train_number is not None:
            row[COL_TRAIN_ID] = ov.to_train_number
        row[COL_LICENSED_TRAIN_ARRIVAL] = ov.to_train_rishui_train_arrival_time or ""
        row["__is_overridden"] = True
    return rows


def _row_year_month_from_mapping(row):
    try:
        return f"{int(row.get(COL_YEAR)):04d}-{int(row.get(COL_MONTH)):02d}"
    except (TypeError, ValueError):
        return ""


def _apply_overrides_to_rows_by_effective_month(rows):
    overrides = list(OverrideConv.objects.order_by("changed_at"))
    if not overrides:
        return rows

    for row in rows:
        row_month = _row_year_month_from_mapping(row)
        if not row_month:
            continue

        key = _row_override_key(row)
        chosen = None
        for ov in overrides:
            if not ov.effective_month or ov.effective_month > row_month:
                continue
            ov_key = (
                str(ov.station_name or "").strip(),
                str(ov.week_period or "").strip(),
                str(ov.link_direction or "").strip(),
                ov.makat,
                ov.direction,
                str(ov.alternative or "").strip(),
                str(ov.departure_time or "").strip(),
                ov.from_train_number,
                str(ov.from_train_rishui_train_arrival_time or "").strip(),
            )
            if ov_key == key:
                chosen = ov

        if chosen is None:
            continue
        if chosen.to_train_number is not None:
            row[COL_TRAIN_ID] = chosen.to_train_number
        row[COL_LICENSED_TRAIN_ARRIVAL] = chosen.to_train_rishui_train_arrival_time or ""
        row["__is_overridden"] = True

    return rows
# endregion override

# region calculating raw percentages (only for bus to rail)
def _hhmmss_to_minutes(value):
    text = str(value or "").strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) < 2:
        return None
    try:
        hours = int(float(parts[0]))
        minutes = int(float(parts[1]))
        seconds = float(parts[2]) if len(parts) > 2 else 0
    except (TypeError, ValueError):
        return None
    return hours * 60 + minutes + seconds / 60


def _diff_minutes(start_value, end_value):
    start = _hhmmss_to_minutes(start_value)
    end = _hhmmss_to_minutes(end_value)
    if start is None or end is None:
        return None
    diff = end - start
    if diff < -720:
        diff += 1440
    if diff > 720:
        diff -= 1440
    return round(diff, 2)


def _raw_lookup_key_from_raw(row):
    return (
        str(row.get("year") or "").strip(),
        _to_int_or_none(row.get("month")),
        str(row.get("train_station_name") or "").strip(),
        str(row.get("week_period") or "").strip(),
        str(row.get("rail_direction") or "").strip(),
        _to_int_or_none(row.get("makat")),
        _to_int_or_none(row.get("direction")),
        str(row.get("alternative") or "").strip(),
        _extract_hhmm(row.get("departure_time")),
    )


def _raw_lookup_key_from_convergence_row(row):
    return (
        str(row.get(COL_YEAR) or "").strip(),
        _to_int_or_none(row.get(COL_MONTH)),
        str(row.get(COL_STATION) or "").strip(),
        str(row.get(COL_WEEK) or "").strip(),
        str(row.get(COL_RAIL_DIR) or "").strip(),
        _to_int_or_none(row.get('מק"ט')),
        _to_int_or_none(row.get("כיוון")),
        str(row.get("חלופה") or "").strip(),
        _extract_hhmm(row.get("שעת יציאה מתחנת המוצא")),
    )


def _add_counts(group, good, total):
    group["good"] += good
    group["total"] += total


def _format_group_percentage(group):
    total = group["total"]
    if total <= 0:
        return ""
    return _format_percentage((group["good"] / total) * 100)


def _attach_calculated_bus_to_rail_percentages(rows, raw_rows):
    raw_lookup = defaultdict(list)
    for raw in raw_rows:
        raw_lookup[_raw_lookup_key_from_raw(raw)].append(raw)

    line_groups = defaultdict(lambda: {"good": 0, "total": 0})
    train_groups = defaultdict(lambda: {"good": 0, "total": 0})
    station_groups = defaultdict(lambda: {"good": 0, "total": 0})
    row_keys = []

    for row in rows:
        matched_raw_rows = raw_lookup.get(_raw_lookup_key_from_convergence_row(row), [])
        train_arrival = row.get(COL_LICENSED_TRAIN_ARRIVAL)
        good = 0
        total = 0

        for raw in matched_raw_rows:
            rides = _to_int_or_none(raw.get("ride_counts"))
            if not rides or rides <= 0:
                continue
            gap = _diff_minutes(raw.get("bus_arrival_time_to_station"), train_arrival)
            if gap is None:
                continue
            total += rides
            if 8 <= gap <= 15:
                good += rides

        line_key = (
            str(row.get(COL_YEAR) or "").strip(),
            _to_int_or_none(row.get(COL_MONTH)),
            str(row.get(COL_STATION) or "").strip(),
            str(row.get(COL_WEEK) or "").strip(),
            str(row.get(COL_RAIL_DIR) or "").strip(),
            _to_int_or_none(row.get(COL_SIGNAGE)),
        )
        train_key = (
            str(row.get(COL_YEAR) or "").strip(),
            _to_int_or_none(row.get(COL_MONTH)),
            str(row.get(COL_STATION) or "").strip(),
            str(row.get(COL_WEEK) or "").strip(),
            str(row.get(COL_RAIL_DIR) or "").strip(),
            _to_int_or_none(row.get(COL_TRAIN_ID)),
            _extract_hhmm(row.get(COL_LICENSED_TRAIN_ARRIVAL)),
        )
        station_key = (
            str(row.get(COL_YEAR) or "").strip(),
            _to_int_or_none(row.get(COL_MONTH)),
            str(row.get(COL_STATION) or "").strip(),
            str(row.get(COL_RAIL_DIR) or "").strip(),
        )

        _add_counts(line_groups[line_key], good, total)
        _add_counts(train_groups[train_key], good, total)
        _add_counts(station_groups[station_key], good, total)
        row_keys.append((row, line_key, train_key, station_key, good, total))

    for row, line_key, train_key, station_key, good, total in row_keys:
        row[COL_N] = total
        row[COL_N_POSITIVE_FLAGGED] = good
        row[COL_PERC] = _format_group_percentage({"good": good, "total": total})
        line_pct = _format_group_percentage(line_groups[line_key])
        row[COL_PERC_BY_MAKAT] = line_pct
        row[COL_PERC_BY_MAKAT_FOR_TREND] = line_pct
        row[COL_PERC_BY_TRAIN] = _format_group_percentage(train_groups[train_key])
        row[COL_PERC_BY_TRAIN_STATION] = _format_group_percentage(station_groups[station_key])

    return rows
# endregion calculating raw percentages (only for bus to rail)

# region color perc for BUS TO RAIL
def _format_color_percentage(green_count, total_count):
    if total_count <= 0:
        return ""
    return f"{(green_count / total_count) * 100:.2f}%"


def _new_color_group():
    return {"green": 0, "total": 0}


def _add_color_count(group, is_green):
    group["total"] += 1
    if is_green:
        group["green"] += 1


def _build_bus_to_rail_dot_color_percentages(rows):
    station_groups = defaultdict(_new_color_group)
    train_groups = defaultdict(_new_color_group)
    signage_groups = defaultdict(_new_color_group)

    for row in rows:
        rail_direction = str(row.get(COL_RAIL_DIR) or "").strip()
        if rail_direction != RAIL_DIRECTION_TO_TLV:
            continue

        train_number = _to_int_or_none(row.get(COL_TRAIN_ID))
        has_train = train_number is not None
        gap = _diff_minutes(row.get(COL_BUS_ARRIVAL_TO_STATION), row.get(COL_LICENSED_TRAIN_ARRIVAL))
        is_green = (
            has_train
            and gap is not None
            and ON_TIME_GAP_MINUTES_LOWER_LIMIT <= gap <= ON_TIME_GAP_MINUTES_UPPER_LIMIT
        )

        station_key = (
            str(row.get(COL_YEAR) or "").strip(),
            _to_int_or_none(row.get(COL_MONTH)),
            str(row.get(COL_STATION) or "").strip(),
            rail_direction,
        )
        signage_key = (
            str(row.get(COL_YEAR) or "").strip(),
            _to_int_or_none(row.get(COL_MONTH)),
            str(row.get(COL_STATION) or "").strip(),
            str(row.get(COL_WEEK) or "").strip(),
            rail_direction,
            _to_int_or_none(row.get(COL_SIGNAGE)),
        )

        _add_color_count(station_groups[station_key], is_green)
        _add_color_count(signage_groups[signage_key], is_green)

        if has_train:
            train_key = (
                str(row.get(COL_YEAR) or "").strip(),
                _to_int_or_none(row.get(COL_MONTH)),
                str(row.get(COL_STATION) or "").strip(),
                str(row.get(COL_WEEK) or "").strip(),
                rail_direction,
                train_number,
                _extract_hhmm(row.get(COL_LICENSED_TRAIN_ARRIVAL)),
            )
            _add_color_count(train_groups[train_key], is_green)

    station_rows = [
        {
            "level": "station",
            "year": year,
            "month": month,
            "station": station,
            "week_period": "",
            "rail_direction": rail_direction,
            "train_number": None,
            "train_arrival_time": "",
            "signage": None,
            "green_count": group["green"],
            "total_count": group["total"],
            "green_percentage": _format_color_percentage(group["green"], group["total"]),
        }
        for (year, month, station, rail_direction), group in station_groups.items()
    ]
    train_rows = [
        {
            "level": "train",
            "year": year,
            "month": month,
            "station": station,
            "week_period": week_period,
            "rail_direction": rail_direction,
            "train_number": train_number,
            "train_arrival_time": train_arrival_time,
            "signage": None,
            "green_count": group["green"],
            "total_count": group["total"],
            "green_percentage": _format_color_percentage(group["green"], group["total"]),
        }
        for (year, month, station, week_period, rail_direction, train_number, train_arrival_time), group in train_groups.items()
    ]
    signage_rows = [
        {
            "level": "signage",
            "year": year,
            "month": month,
            "station": station,
            "week_period": week_period,
            "rail_direction": rail_direction,
            "train_number": None,
            "train_arrival_time": "",
            "signage": signage,
            "green_count": group["green"],
            "total_count": group["total"],
            "green_percentage": _format_color_percentage(group["green"], group["total"]),
        }
        for (year, month, station, week_period, rail_direction, signage), group in signage_groups.items()
    ]

    def sort_key(item):
        return (
            str(item["year"] or ""),
            item["month"] if item["month"] is not None else -1,
            str(item["station"] or ""),
            str(item["week_period"] or ""),
            item["train_number"] if item["train_number"] is not None else -1,
            item["signage"] if item["signage"] is not None else -1,
            str(item["train_arrival_time"] or ""),
        )

    return {
        "station": sorted(station_rows, key=sort_key),
        "train": sorted(train_rows, key=sort_key),
        "signage": sorted(signage_rows, key=sort_key),
    }

# endregion color perc for BUS TO RAIL

# region color perc for RAIL TO BUS
def _build_rail_to_bus_dot_color_percentages(rows):
    station_groups = defaultdict(_new_color_group)
    train_groups = defaultdict(_new_color_group)
    signage_groups = defaultdict(_new_color_group)

    for row in rows:
        rail_direction = str(row.get(COL_RAIL_DIR) or "").strip()
        if rail_direction == RAIL_DIRECTION_TO_TLV:
            continue

        train_number = _to_int_or_none(row.get(COL_TRAIN_ID))
        has_train = train_number is not None
        gap = _diff_minutes(row.get("שעת יציאה מתחנת המוצא"), row.get(COL_LICENSED_TRAIN_ARRIVAL))
        is_green = (
            has_train
            and gap is not None
            and ON_TIME_GAP_MINUTES_LOWER_LIMIT <= gap <= ON_TIME_GAP_MINUTES_UPPER_LIMIT
        )

        station_key = (
            str(row.get(COL_YEAR) or "").strip(),
            _to_int_or_none(row.get(COL_MONTH)),
            str(row.get(COL_STATION) or "").strip(),
            rail_direction,
        )
        signage_key = (
            str(row.get(COL_YEAR) or "").strip(),
            _to_int_or_none(row.get(COL_MONTH)),
            str(row.get(COL_STATION) or "").strip(),
            str(row.get(COL_WEEK) or "").strip(),
            rail_direction,
            _to_int_or_none(row.get(COL_SIGNAGE)),
        )

        _add_color_count(station_groups[station_key], is_green)
        _add_color_count(signage_groups[signage_key], is_green)

        if has_train:
            train_key = (
                str(row.get(COL_YEAR) or "").strip(),
                _to_int_or_none(row.get(COL_MONTH)),
                str(row.get(COL_STATION) or "").strip(),
                str(row.get(COL_WEEK) or "").strip(),
                rail_direction,
                train_number,
                _extract_hhmm(row.get(COL_LICENSED_TRAIN_ARRIVAL)),
            )
            _add_color_count(train_groups[train_key], is_green)

    station_rows = [
        {
            "level": "station",
            "year": year,
            "month": month,
            "station": station,
            "week_period": "",
            "rail_direction": rail_direction,
            "train_number": None,
            "train_arrival_time": "",
            "signage": None,
            "green_count": group["green"],
            "total_count": group["total"],
            "green_percentage": _format_color_percentage(group["green"], group["total"]),
        }
        for (year, month, station, rail_direction), group in station_groups.items()
    ]
    train_rows = [
        {
            "level": "train",
            "year": year,
            "month": month,
            "station": station,
            "week_period": week_period,
            "rail_direction": rail_direction,
            "train_number": train_number,
            "train_arrival_time": train_arrival_time,
            "signage": None,
            "green_count": group["green"],
            "total_count": group["total"],
            "green_percentage": _format_color_percentage(group["green"], group["total"]),
        }
        for (year, month, station, week_period, rail_direction, train_number, train_arrival_time), group in train_groups.items()
    ]
    signage_rows = [
        {
            "level": "signage",
            "year": year,
            "month": month,
            "station": station,
            "week_period": week_period,
            "rail_direction": rail_direction,
            "train_number": None,
            "train_arrival_time": "",
            "signage": signage,
            "green_count": group["green"],
            "total_count": group["total"],
            "green_percentage": _format_color_percentage(group["green"], group["total"]),
        }
        for (year, month, station, week_period, rail_direction, signage), group in signage_groups.items()
    ]

    def sort_key(item):
        return (
            str(item["year"] or ""),
            item["month"] if item["month"] is not None else -1,
            str(item["station"] or ""),
            str(item["week_period"] or ""),
            item["train_number"] if item["train_number"] is not None else -1,
            item["signage"] if item["signage"] is not None else -1,
            str(item["train_arrival_time"] or ""),
        )

    return {
        "station": sorted(station_rows, key=sort_key),
        "train": sorted(train_rows, key=sort_key),
        "signage": sorted(signage_rows, key=sort_key),
    }
# endregion color perc for RAIL TO BUS



@require_POST
@login_required
@permission_required("convergence.can_manage_convergence_overrides", raise_exception=True)
def save_override(request):
    try:
        payload = json.loads((request.body or b"").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse({"ok": False, "error": "invalid_json"}, status=400)

    required = (
        "week_period",
        "link_direction",
        "makat",
        "direction",
        "station_name",
        "from_train_number",
        "to_train_number",
        "to_train_rishui_train_arrival_time",
        "effective_month",
    )
    missing = [k for k in required if payload.get(k) in (None, "")]
    if missing:
        return JsonResponse({"ok": False, "error": "missing_fields", "fields": missing}, status=400)

    link_direction = str(payload.get("link_direction") or "").strip()
    if link_direction not in ("bus_to_rail", "rail_to_bus"):
        return JsonResponse({"ok": False, "error": "invalid_link_direction"}, status=400)

    defaults = {
        "station_name": str(payload.get("station_name")).strip(),
        "to_departure_time": str(payload.get("to_departure_time") or "").strip(),
        "to_train_number": _to_int_or_none(payload.get("to_train_number")),
        "to_train_rishui_train_arrival_time": str(payload.get("to_train_rishui_train_arrival_time") or "").strip(),
        "effective_month": str(payload.get("effective_month") or "").strip(),
        "change_reason": str(payload.get("change_reason") or "").strip(),
        "changed_by": request.user.username,
        "changed_at": timezone.localtime(timezone.now()).replace(microsecond=0),
    }

    if defaults["to_train_number"] is None:
        return JsonResponse({"ok": False, "error": "invalid_to_train_number"}, status=400)
    if len(defaults["effective_month"]) != 7:
        return JsonResponse({"ok": False, "error": "invalid_effective_month"}, status=400)

    lookup = {
        "week_period": str(payload.get("week_period") or "").strip(),
        "link_direction": link_direction,
        "makat": _to_int_or_none(payload.get("makat")),
        "direction": _to_int_or_none(payload.get("direction")),
        "alternative": str(payload.get("alternative") or "").strip(),
        "departure_time": str(payload.get("departure_time") or "").strip(),
        "station_name": str(payload.get("station_name") or "").strip(),
        "from_train_number": _to_int_or_none(payload.get("from_train_number")),
        "from_train_rishui_train_arrival_time": str(payload.get("from_train_rishui_train_arrival_time") or "").strip(),
    }
    must_exist = ("week_period", "link_direction", "makat", "direction", "station_name", "from_train_number")
    bad_lookup = [k for k in must_exist if lookup.get(k) in (None, "")]
    if bad_lookup:
        return JsonResponse({"ok": False, "error": "invalid_lookup_fields", "fields": bad_lookup}, status=400)

    obj, created = OverrideConv.objects.update_or_create(**lookup, defaults=defaults)
    return JsonResponse({"ok": True, "id": obj.id, "created": created})


@require_GET
def line_history(request):
    station = (request.GET.get("station") or "").strip()
    link_direction = (request.GET.get("link_direction") or "").strip()
    week_period = (request.GET.get("week_period") or "").strip()
    makat = _to_int_or_none(request.GET.get("makat"))
    direction = _to_int_or_none(request.GET.get("direction"))
    alternative = (request.GET.get("alternative") or "").strip()
    departure_time = _extract_hhmm(request.GET.get("departure_time"))

    required_missing = []
    if not station:
        required_missing.append("station")
    if link_direction not in ("bus_to_rail", "rail_to_bus"):
        required_missing.append("link_direction")
    if not week_period:
        required_missing.append("week_period")
    if makat is None:
        required_missing.append("makat")
    if direction is None:
        required_missing.append("direction")
    if not departure_time:
        required_missing.append("departure_time")
    if required_missing:
        return JsonResponse({"ok": False, "error": "missing_or_invalid_fields", "fields": required_missing}, status=400)

    model = ConvergenceBusToRail if link_direction == "bus_to_rail" else ConvergenceRailToBus
    qs = (
        model.objects
        .annotate(_station_trim=Trim("train_station_name"))
        .filter(
            _station_trim=station,
            week_period=week_period,
            makat=makat,
            direction=direction,
            alternative=alternative,
        )
        .order_by("year", "month", "train_number", "rishui_train_arrival_time")
    )

    base_rows = [
        row for row in qs
        if _extract_hhmm(row.departure_time) == departure_time
    ]

    rows = []
    for row in base_rows:
        year_month = _row_year_month(row)
        original_train_number = row.train_number
        original_arrival = _extract_hhmm(row.rishui_train_arrival_time)
        original_departure = str(row.departure_time or "").strip()

        override = (
            OverrideConv.objects
            .filter(
                station_name=station,
                week_period=week_period,
                link_direction=link_direction,
                makat=makat,
                direction=direction,
                alternative=alternative,
                departure_time__in=[departure_time, original_departure],
                from_train_number=original_train_number,
                from_train_rishui_train_arrival_time__in=[
                    str(row.rishui_train_arrival_time or "").strip(),
                    original_arrival,
                ],
                effective_month__lte=year_month,
            )
            .order_by("changed_at")
            .last()
        )

        train_id = original_train_number
        train_arrival_time = original_arrival
        if override is not None:
            if override.to_train_number is not None:
                train_id = override.to_train_number
            train_arrival_time = _extract_hhmm(override.to_train_rishui_train_arrival_time)

        rows.append({
            "year_month": year_month,
            "train_station": row.train_station_name,
            "link_direction": link_direction,
            "week_period": row.week_period,
            "makat": row.makat,
            "direction": row.direction,
            "alternative": row.alternative,
            "departure_time": departure_time,
            "train_id": train_id,
            "train_arrival_time_rishui": train_arrival_time,
        })

    rows.sort(key=lambda item: (item["year_month"], str(item["train_id"] or ""), item["train_arrival_time_rishui"]))
    for idx, row in enumerate(rows, start=1):
        row["id"] = idx

    return JsonResponse({"ok": True, "rows": rows})

# endregion override

# region RawBusData
def _serialize_raw_bus_data(row):
    return {
        "year": row.year,
        "month": row.month,
        "week_period": row.week_period,
        "train_station_name": row.train_station_name,
        "makat": row.makat,
        "direction": row.direction,
        "alternative": row.alternative,
        "departure_time": row.departure_time,
        "bus_arrival_time_to_station": row.bus_arrival_time_to_station,
        "ride_counts": row.ride_counts,
        "rail_direction": row.rail_direction,
    }

# endregion RawBusData

def convergence(request):
    station = (request.GET.get("station") or "").strip()

    y = (request.GET.get("year") or "").strip()
    year = int(y) if y.isdigit() else None

    m = (request.GET.get("month") or "").strip()
    month = int(m) if m.isdigit() else None

    if not station:
        return render(
            request,
            "convergence.html",
            {
                "debug_message": "missing station in URL",
                "station": "",
                "year": "",
                "month": "",
                "bus_to_rail_df": [],
                "bus_to_rail_trend_df": [],
                "bus_to_rail_dot_color_percentages_df": {"station": [], "train": [], "signage": []},
                "rail_to_bus_df": [],
                "raw_bus_data_df": [],
                "year_month_pairs": [],
            },
        )

    bus_qs = ConvergenceBusToRail.objects.annotate(_station_trim=Trim("train_station_name")).filter(_station_trim=station)
    rail_qs = ConvergenceRailToBus.objects.annotate(_station_trim=Trim("train_station_name")).filter(_station_trim=station)
    raw_qs = RawBusData.objects.annotate(_station_trim=Trim("train_station_name")).filter(_station_trim=station)

    if not bus_qs.exists() and not rail_qs.exists() and not raw_qs.exists():
        bus_qs = ConvergenceBusToRail.objects.filter(train_station_name__icontains=station)
        rail_qs = ConvergenceRailToBus.objects.filter(train_station_name__icontains=station)
        raw_qs = RawBusData.objects.filter(train_station_name__icontains=station)

    bus_qs_for_trend = bus_qs
    raw_qs_for_trend = raw_qs
    rail_qs_for_trend = rail_qs

    year_month_pairs_set = set()
    for yv, mv in bus_qs.values_list("year", "month"):
        if yv is not None and mv is not None:
            year_month_pairs_set.add((int(yv), int(mv)))
    for yv, mv in rail_qs.values_list("year", "month"):
        if yv is not None and mv is not None:
            year_month_pairs_set.add((int(yv), int(mv)))
    for yv, mv in raw_qs.values_list("year", "month"):
        if yv is not None and mv is not None:
            try:
                year_month_pairs_set.add((int(yv), int(mv)))
            except (TypeError, ValueError):
                pass

    year_month_pairs = [
        {"year": yv, "month": mv}
        for (yv, mv) in sorted(year_month_pairs_set)
    ]

    if (year is None or month is None) and year_month_pairs:
        if year is None:
            year = year_month_pairs[0]["year"]
        if month is None:
            month = year_month_pairs[0]["month"]

    if year is not None:
        bus_qs = bus_qs.filter(year=year)
        rail_qs = rail_qs.filter(year=year)
        raw_qs = raw_qs.filter(year=str(year))
    if month is not None:
        bus_qs = bus_qs.filter(month=month)
        rail_qs = rail_qs.filter(month=month)
        raw_qs = raw_qs.filter(month=month)

    bus_to_rail_trend_rows = [_serialize_bus_to_rail_trend(row) for row in bus_qs_for_trend]
    rail_to_bus_trend_rows = [_serialize_rail_to_bus(row) for row in rail_qs_for_trend]
    bus_to_rail_rows = [_serialize_bus_to_rail(row) for row in bus_qs]
    rail_to_bus_rows = [_serialize_rail_to_bus(row) for row in rail_qs]
    raw_bus_data_rows = [_serialize_raw_bus_data(row) for row in raw_qs]
    raw_bus_data_trend_rows = [_serialize_raw_bus_data(row) for row in raw_qs_for_trend]

    effective_month = ""
    if year is not None and month is not None:
        effective_month = f"{int(year):04d}-{int(month):02d}"

    overrides = _build_override_lookup(effective_month)
    _apply_overrides_to_rows(bus_to_rail_rows, overrides)
    _apply_overrides_to_rows(rail_to_bus_rows, overrides)
    _apply_overrides_to_rows_by_effective_month(bus_to_rail_trend_rows)
    _apply_overrides_to_rows_by_effective_month(rail_to_bus_trend_rows)
    _attach_calculated_bus_to_rail_percentages(bus_to_rail_rows, raw_bus_data_rows)
    _attach_calculated_bus_to_rail_percentages(bus_to_rail_trend_rows, raw_bus_data_trend_rows)
    bus_to_rail_dot_color_percentages = _build_bus_to_rail_dot_color_percentages(bus_to_rail_trend_rows)
    rail_to_bus_dot_color_percentages = _build_rail_to_bus_dot_color_percentages(rail_to_bus_trend_rows)


    debug_message = ""
    if not bus_to_rail_rows and not rail_to_bus_rows and not raw_bus_data_rows:
        debug_message = f"no convergence rows found for station='{station}', year='{year}', month='{month}'"

    context = {
        "debug_message": debug_message,
        "station": station,
        "year": year or "",
        "month": month or "",
        "bus_to_rail_df": bus_to_rail_rows,
        "bus_to_rail_trend_df": bus_to_rail_trend_rows,
        "bus_to_rail_dot_color_percentages_df": bus_to_rail_dot_color_percentages,
        "rail_to_bus_dot_color_percentages_df": rail_to_bus_dot_color_percentages,
        "rail_to_bus_df": rail_to_bus_rows,
        "raw_bus_data_df": raw_bus_data_rows,
        "year_month_pairs": year_month_pairs,
    }
    return render(request, "convergence.html", context)
