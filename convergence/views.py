import json
from decimal import Decimal

from django.db.models.functions import Trim
from django.shortcuts import render

from convergence.models import ConvergenceBusToRail, ConvergenceRailToBus


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
COL_LICENSED_TRAIN_DEPARTURE = "זמן יציאת הרכבת מהתחנה (רישוי)"
COL_LICENSED_TRAIN_ARRIVAL = "זמן הגעת הרכבת לתחנה (רישוי)"

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


def _serialize_bus_row(row):
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
        COL_LICENSED_TRAIN_DEPARTURE: row.rishui_train_departure_time,
        COL_LICENSED_TRAIN_ARRIVAL: "",
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


def _serialize_rail_row(row):
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
        COL_LICENSED_TRAIN_DEPARTURE: "",
        COL_LICENSED_TRAIN_ARRIVAL: row.rishui_train_arrival_time,
        "מפעיל": row.operator,
        'מק"ט': row.makat,
        "כיוון": row.direction,
        "חלופה": row.alternative,
        "זמן יציאה": row.departure_time,
        "ממוצע נוסעים לנסיעה": row.avg_passengers_per_trip,
        "זמן יציאה מהתחנה (רישוי)": row.train_departure_time,
        "הפרש בדקות (מרכבת לאוטובוס)": row.minutes_gap_rail_to_bus,
        "המלצה (דקות)": row.recommended_minutes,
    }


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
                "year_month_pairs_js": "[]",
            },
        )

    bus_qs = ConvergenceBusToRail.objects.annotate(_station_trim=Trim("train_station_name")).filter(_station_trim=station)
    rail_qs = ConvergenceRailToBus.objects.annotate(_station_trim=Trim("train_station_name")).filter(_station_trim=station)

    if not bus_qs.exists() and not rail_qs.exists():
        bus_qs = ConvergenceBusToRail.objects.filter(train_station_name__icontains=station)
        rail_qs = ConvergenceRailToBus.objects.filter(train_station_name__icontains=station)

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

    bus_rows = [_serialize_bus_row(row) for row in bus_qs]
    rail_rows = [_serialize_rail_row(row) for row in rail_qs]

    combined_for_perc = bus_rows + rail_rows
    train_perc_map = _build_train_perc_map(combined_for_perc)

    debug_message = ""
    if not bus_rows and not rail_rows:
        debug_message = f"no convergence rows found for station='{station}', year='{year}', month='{month}'"

    context = {
        "debug_message": debug_message,
        "station": station,
        "year": year or "",
        "month": month or "",
        "bus_to_rail_df_js": json.dumps(bus_rows, ensure_ascii=False),
        "rail_to_bus_df_js": json.dumps(rail_rows, ensure_ascii=False),
        "train_perc_js": json.dumps(train_perc_map, ensure_ascii=False),
        "year_month_pairs_js": json.dumps(year_month_pairs, ensure_ascii=False),
    }
    return render(request, "convergence.html", context)
