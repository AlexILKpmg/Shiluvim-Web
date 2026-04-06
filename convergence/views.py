import json
from decimal import Decimal

from django.db.models.functions import Trim
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_POST

from convergence.models import ConvergenceBusToRail, ConvergenceRailToBus, OverrideConv


COL_STATION = "שם תחנת הרכבת"
COL_YEAR = "שנה"
COL_MONTH = "חודש"
COL_WEEK = "תקופת שבוע"
COL_RAIL_DIR = "כיוון נסיעת הרכבת"
COL_TRAIN_ID = "מספר הרכבת"
COL_PERC = "אחוז הנסיעות שעמדו בזמנים"
COL_N = "מספר תצפיות"
COL_N_POSITIVE_FLAGGED = "מספר הנסיעות שעמדו בזמנים"
COL_SIGNAGE = "שילוט"
COL_GOLD_TRAIN = "רכבת זהב"
COL_BUS_ON_TIME = "האם האוטובוס מגיע בזמן"
COL_LICENSED_TRAIN_ARRIVAL = "זמן הגעת הרכבת לתחנה (רישוי)"

COL_TRAIN_STATION_CODE = "__train_station_code"
COL_FROM_TRAIN_NUMBER = "__from_train_number"
COL_FROM_TRAIN_ARRIVAL = "__from_train_rishui_train_arrival_time"
COL_LINK_DIRECTION = "__link_direction"

DIR_TO_TA = "לכיוון תל אביב"
DIR_FROM_TA = "מכיוון תל אביב"


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
        COL_BUS_ON_TIME: row.is_bus_on_time,
        COL_LICENSED_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_TRAIN_STATION_CODE: row.train_station_code,
        COL_FROM_TRAIN_NUMBER: row.train_number,
        COL_FROM_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_LINK_DIRECTION: "bus_to_rail",
        "מספר עולים": row.train_ascending_amount,
        "מפעיל": row.operator,
        'מק"ט': row.makat,
        "כיוון": row.direction,
        "חלופה": row.alternative,
        "זמן יציאה": row.departure_time,
        "ממוצע נוסעים לנסיעה": row.avg_passengers_per_trip,
        "זמן הגעה לתחנה": row.arrival_time_to_station,
        "טווח זמן ההגעה לתחנה": row.arrival_time_window,
        "הפרש בדקות (מאוטובוס לרכבת)": row.minutes_gap_bus_to_rail,
        "המלצה (דקות)": row.recommended_minutes,
        COL_N: row.observations_count,
        COL_N_POSITIVE_FLAGGED: row.on_time_count,
        COL_PERC: _format_percentage(row.on_time_percentage),
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
        COL_BUS_ON_TIME: row.is_bus_on_time,
        COL_LICENSED_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_TRAIN_STATION_CODE: row.train_station_code,
        COL_FROM_TRAIN_NUMBER: row.train_number,
        COL_FROM_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        COL_LINK_DIRECTION: "rail_to_bus",
        "מספר יורדים": row.train_descending_amount,
        "מפעיל": row.operator,
        'מק"ט': row.makat,
        "כיוון": row.direction,
        "חלופה": row.alternative,
        "זמן יציאה": row.departure_time,
        "ממוצע נוסעים לנסיעה": row.avg_passengers_per_trip,
        "הפרש בדקות (מרכבת לאוטובוס)": row.minutes_gap_rail_to_bus,
        "המלצה (דקות)": row.recommended_minutes,
    }


def _row_override_key(row):
    return (
        str(row.get(COL_WEEK) or "").strip(),
        str(row.get(COL_LINK_DIRECTION) or "").strip(),
        _to_int_or_none(row.get('מק"ט')),
        _to_int_or_none(row.get("כיוון")),
        str(row.get("חלופה") or "").strip(),
        str(row.get("זמן יציאה") or "").strip(),
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


def _build_train_perc_map(rows):
    grouped = {}

    for row in rows:
        year = row.get(COL_YEAR)
        month = row.get(COL_MONTH)
        train_id = str(row.get(COL_TRAIN_ID) or "").strip()
        rail_dir = str(row.get(COL_RAIL_DIR) or "").strip()

        n = row.get(COL_N)
        positive = row.get(COL_N_POSITIVE_FLAGGED)

        if year is None or month is None or not train_id or not rail_dir:
            continue
        if n is None or positive is None:
            continue

        try:
            n_val = float(n)
            p_val = float(positive)
        except (TypeError, ValueError):
            continue

        if n_val <= 0:
            continue

        key = (int(year), int(month), train_id, rail_dir)
        if key not in grouped:
            grouped[key] = [0.0, 0.0]
        grouped[key][0] += p_val
        grouped[key][1] += n_val

    out = {}
    for (year, month, train_id, rail_dir), (p_sum, n_sum) in grouped.items():
        if n_sum <= 0:
            continue
        out[f"{year}_{month}_{train_id}_{rail_dir}"] = f"{(p_sum / n_sum) * 100:.1f}%"

    return out


def _build_train_trend_map(rows):
    grouped = {}

    for row in rows:
        year = row.get(COL_YEAR)
        month = row.get(COL_MONTH)
        train_id = str(row.get(COL_TRAIN_ID) or "").strip()
        rail_dir = str(row.get(COL_RAIL_DIR) or "").strip()

        n = row.get(COL_N)
        positive = row.get(COL_N_POSITIVE_FLAGGED)

        if year is None or month is None or not train_id or not rail_dir:
            continue
        if n is None or positive is None:
            continue

        try:
            year_i = int(year)
            month_i = int(month)
            n_val = float(n)
            p_val = float(positive)
        except (TypeError, ValueError):
            continue

        if n_val <= 0:
            continue

        key = (train_id, rail_dir, year_i, month_i)
        if key not in grouped:
            grouped[key] = [0.0, 0.0]
        grouped[key][0] += p_val
        grouped[key][1] += n_val

    per_train = {}
    for (train_id, rail_dir, year_i, month_i), (p_sum, n_sum) in grouped.items():
        if n_sum <= 0:
            continue

        series_key = f"{train_id}||{rail_dir}"
        if series_key not in per_train:
            per_train[series_key] = []
        per_train[series_key].append(
            {
                "year": year_i,
                "month": month_i,
                "perc": round((p_sum / n_sum) * 100, 1),
            }
        )

    for series in per_train.values():
        series.sort(key=lambda p: (p["year"], p["month"]))

    return per_train


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
                "bus_to_rail_df_js": "[]",
                "rail_to_bus_df_js": "[]",
                "train_perc_js": "{}",
                "train_trend_js": "{}",
                "year_month_pairs_js": "[]",
            },
        )

    bus_qs = ConvergenceBusToRail.objects.annotate(_station_trim=Trim("train_station_name")).filter(_station_trim=station)
    rail_qs = ConvergenceRailToBus.objects.annotate(_station_trim=Trim("train_station_name")).filter(_station_trim=station)

    if not bus_qs.exists() and not rail_qs.exists():
        bus_qs = ConvergenceBusToRail.objects.filter(train_station_name__icontains=station)
        rail_qs = ConvergenceRailToBus.objects.filter(train_station_name__icontains=station)

    bus_qs_all = bus_qs
    rail_qs_all = rail_qs

    year_month_pairs_set = set()
    for yv, mv in bus_qs.values_list("year", "month"):
        if yv is not None and mv is not None:
            year_month_pairs_set.add((int(yv), int(mv)))
    for yv, mv in rail_qs.values_list("year", "month"):
        if yv is not None and mv is not None:
            year_month_pairs_set.add((int(yv), int(mv)))

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
    if month is not None:
        bus_qs = bus_qs.filter(month=month)
        rail_qs = rail_qs.filter(month=month)

    bus_to_rail_rows = [_serialize_bus_to_rail(row) for row in bus_qs]
    rail_to_bus_rows = [_serialize_rail_to_bus(row) for row in rail_qs]
    bus_to_rail_rows_all = [_serialize_bus_to_rail(row) for row in bus_qs_all]
    rail_to_bus_rows_all = [_serialize_rail_to_bus(row) for row in rail_qs_all]

    effective_month = ""
    if year is not None and month is not None:
        effective_month = f"{int(year):04d}-{int(month):02d}"

    overrides = _build_override_lookup(effective_month)
    _apply_overrides_to_rows(bus_to_rail_rows, overrides)
    _apply_overrides_to_rows(rail_to_bus_rows, overrides)

    combined_for_perc = bus_to_rail_rows + rail_to_bus_rows
    train_perc_map = _build_train_perc_map(combined_for_perc)
    train_trend_map = _build_train_trend_map(bus_to_rail_rows_all + rail_to_bus_rows_all)

    debug_message = ""
    if not bus_to_rail_rows and not rail_to_bus_rows:
        debug_message = f"no convergence rows found for station='{station}', year='{year}', month='{month}'"

    context = {
        "debug_message": debug_message,
        "station": station,
        "year": year or "",
        "month": month or "",
        "bus_to_rail_df_js": json.dumps(bus_to_rail_rows, ensure_ascii=False),
        "rail_to_bus_df_js": json.dumps(rail_to_bus_rows, ensure_ascii=False),
        "train_perc_js": json.dumps(train_perc_map, ensure_ascii=False),
        "train_trend_js": json.dumps(train_trend_map, ensure_ascii=False),
        "year_month_pairs_js": json.dumps(year_month_pairs, ensure_ascii=False),
    }
    return render(request, "convergence.html", context)


@require_POST
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
        "to_train_number": _to_int_or_none(payload.get("to_train_number")),
        "to_train_rishui_train_arrival_time": str(payload.get("to_train_rishui_train_arrival_time") or "").strip(),
        "effective_month": str(payload.get("effective_month") or "").strip(),
        "change_reason": str(payload.get("change_reason") or "").strip(),
        "changed_by": str(payload.get("changed_by") or "").strip(),
        "changed_at": timezone.now(),
        "is_enabled": True,
        "disabled_at": None,
        "disabled_by": "",
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
    obj.disabled_at = timezone.now()
    obj.disabled_by = str(payload.get("disabled_by") or "").strip()
    obj.disable_reason = str(payload.get("disable_reason") or "").strip()
    obj.save(update_fields=("is_enabled", "disabled_at", "disabled_by", "disable_reason"))

    return JsonResponse({"ok": True, "id": obj.id})

