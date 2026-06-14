import json
from decimal import Decimal

from django.contrib.auth.decorators import login_required, permission_required
from django.db.models.functions import Trim
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_POST

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
        COL_PERC: _format_percentage(row.on_time_percentage),
        COL_PERC_BY_TRAIN: _format_percentage(row.on_time_percentage_by_train),
        COL_PERC_BY_MAKAT: _format_percentage(row.on_time_percentage_by_makat),
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
        COL_TRAIN_ID: row.train_number,
        COL_LICENSED_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_SIGNAGE: row.signage,
        COL_PERC_BY_MAKAT_FOR_TREND: _format_percentage(row.on_time_percentage_by_makat),
        COL_PERC_BY_TRAIN: _format_percentage(row.on_time_percentage_by_train),
        COL_PERC_BY_TRAIN_STATION: _format_percentage(row.on_time_percentage_by_train_station),
    }

# endregion organizing the data from DB

# region override
def _row_override_key(row):
    return (
        str(row.get(COL_WEEK) or "").strip(),
        str(row.get(COL_LINK_DIRECTION) or "").strip(),
        _to_int_or_none(row.get('מק"ט')),
        _to_int_or_none(row.get("כיוון")),
        str(row.get("חלופה") or "").strip(),
        str(row.get("שעת יציאה מתחנת המוצא") or "").strip(),
        _to_int_or_none(row.get(COL_TRAIN_STATION_CODE)),
        _to_int_or_none(row.get(COL_FROM_TRAIN_NUMBER)),
        str(row.get(COL_FROM_TRAIN_ARRIVAL) or "").strip(),
    )


def _build_override_lookup(effective_month):
    out = {}
    if not effective_month:
        return out

    qs = OverrideConv.objects.filter(is_enabled=True, effective_month__lte=effective_month).order_by("changed_at")

    for ov in qs:
        key = (
            str(ov.week_period or "").strip(),
            str(ov.link_direction or "").strip(),
            ov.makat,
            ov.direction,
            str(ov.alternative or "").strip(),
            str(ov.departure_time or "").strip(),
            ov.train_station_code,
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
        "train_station_code",
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
        "to_departure_time": str(payload.get("to_departure_time") or "").strip(),
        "to_train_number": _to_int_or_none(payload.get("to_train_number")),
        "to_train_rishui_train_arrival_time": str(payload.get("to_train_rishui_train_arrival_time") or "").strip(),
        "effective_month": str(payload.get("effective_month") or "").strip(),
        "change_reason": str(payload.get("change_reason") or "").strip(),
        "changed_by": request.user.username,
        "changed_at": timezone.now().replace(microsecond=0),
        "is_enabled": True,
        "disabled_at": None,
        "disabled_by": request.user.username,
        "disable_reason": "",
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
        "train_station_code": _to_int_or_none(payload.get("train_station_code")),
        "from_train_number": _to_int_or_none(payload.get("from_train_number")),
        "from_train_rishui_train_arrival_time": str(payload.get("from_train_rishui_train_arrival_time") or "").strip(),
    }
    must_exist = ("week_period", "link_direction", "makat", "direction", "train_station_code", "from_train_number")
    bad_lookup = [k for k in must_exist if lookup.get(k) in (None, "")]
    if bad_lookup:
        return JsonResponse({"ok": False, "error": "invalid_lookup_fields", "fields": bad_lookup}, status=400)

    obj, created = OverrideConv.objects.update_or_create(**lookup, defaults=defaults)
    return JsonResponse({"ok": True, "id": obj.id, "created": created})


@require_POST
@login_required
@permission_required("convergence.can_manage_convergence_overrides", raise_exception=True)
def disable_override(request):
    try:
        payload = json.loads((request.body or b"").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse({"ok": False, "error": "invalid_json"}, status=400)

    lookup = {
        "week_period": str(payload.get("week_period") or "").strip(),
        "link_direction": str(payload.get("link_direction") or "").strip(),
        "makat": _to_int_or_none(payload.get("makat")),
        "direction": _to_int_or_none(payload.get("direction")),
        "alternative": str(payload.get("alternative") or "").strip(),
        "departure_time": str(payload.get("departure_time") or "").strip(),
        "train_station_code": _to_int_or_none(payload.get("train_station_code")),
        "from_train_number": _to_int_or_none(payload.get("from_train_number")),
        "from_train_rishui_train_arrival_time": str(payload.get("from_train_rishui_train_arrival_time") or "").strip(),
    }

    must_exist = ("week_period", "link_direction", "makat", "direction", "train_station_code", "from_train_number")
    bad_lookup = [k for k in must_exist if lookup.get(k) in (None, "")]
    if bad_lookup:
        return JsonResponse({"ok": False, "error": "invalid_lookup_fields", "fields": bad_lookup}, status=400)

    obj = OverrideConv.objects.filter(**lookup).first()
    if obj is None:
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    obj.is_enabled = False
    obj.disabled_at = timezone.now().replace(microsecond=0)
    obj.disabled_by = str(payload.get("disabled_by") or "").strip()
    obj.disable_reason = str(payload.get("disable_reason") or "").strip()
    obj.save(update_fields=("is_enabled", "disabled_at", "disabled_by", "disable_reason"))

    return JsonResponse({"ok": True, "id": obj.id})


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
    bus_to_rail_rows = [_serialize_bus_to_rail(row) for row in bus_qs]
    rail_to_bus_rows = [_serialize_rail_to_bus(row) for row in rail_qs]
    raw_bus_data_rows = [_serialize_raw_bus_data(row) for row in raw_qs]

    effective_month = ""
    if year is not None and month is not None:
        effective_month = f"{int(year):04d}-{int(month):02d}"

    overrides = _build_override_lookup(effective_month)
    _apply_overrides_to_rows(bus_to_rail_rows, overrides)
    _apply_overrides_to_rows(rail_to_bus_rows, overrides)


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
        "rail_to_bus_df": rail_to_bus_rows,
        "raw_bus_data_df": raw_bus_data_rows,
        "year_month_pairs": year_month_pairs,
    }
    return render(request, "convergence.html", context)

