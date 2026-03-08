from django.shortcuts import render

from train_times.models import TrainTime


def _format_time(value):
    if value is None:
        return ""
    try:
        return value.strftime("%H:%M:%S")
    except Exception:
        return str(value)


def train_times(request):
    station = (request.GET.get("station") or "").strip()

    y = (request.GET.get("year") or "").strip()
    year = int(y) if y.isdigit() else None

    m = (request.GET.get("month") or "").strip()
    month = int(m) if m.isdigit() else None

    debug_message = ""

    if not station or year is None or month is None:
        return render(
            request,
            "train_times.html",
            {
                "debug_message": "חסר station/year/month ב-URL, לכן לא מוצגים נתונים.",
                "station": station or "",
                "year": year or "",
                "month": month or "",
                "arr_rows": [],
                "dep_rows": [],
            },
        )

    base_qs = TrainTime.objects.all()

    station_options = sorted(
        {
            s.strip()
            for s in base_qs.values_list("StationName", flat=True)
            if isinstance(s, str) and s.strip()
        }
    )

    station_qs = base_qs.filter(StationName=station)
    year_options = sorted(
        {
            int(v)
            for v in station_qs.values_list("Year", flat=True).distinct()
            if v is not None
        }
    )

    if year is not None and year not in year_options:
        year = None

    if year is not None:
        month_options = sorted(
            {
                int(v)
                for v in station_qs.filter(Year=year).values_list("Month", flat=True).distinct()
                if v is not None
            }
        )
    else:
        month_options = []

    if month is not None and month not in month_options:
        month = None

    arr_qs = base_qs.filter(event_type=TrainTime.EventType.ARRIVAL)
    dep_qs = base_qs.filter(event_type=TrainTime.EventType.DEPARTURE)

    if station:
        arr_qs = arr_qs.filter(StationName=station)
        dep_qs = dep_qs.filter(StationName=station)
    if year is not None:
        arr_qs = arr_qs.filter(Year=year)
        dep_qs = dep_qs.filter(Year=year)
    if month is not None:
        arr_qs = arr_qs.filter(Month=month)
        dep_qs = dep_qs.filter(Month=month)

    arr_rows = [
        {
            "Year": row["Year"],
            "Month": row["Month"],
            "WeekPeriod": row["WeekPeriod"],
            "train_station_code": row["train_station_code"],
            "StationName": row["StationName"],
            "Train_number": row["Train_number"],
            "Planned_Train_Arrivel_Time": _format_time(row["planned_time"]),
            "PassengersAscending": row["PassengersAscending"],
            "PassengersDescending": row["PassengersDescending"],
        }
        for row in arr_qs.values(
            "Year",
            "Month",
            "WeekPeriod",
            "train_station_code",
            "StationName",
            "Train_number",
            "planned_time",
            "PassengersAscending",
            "PassengersDescending",
        )
    ]

    dep_rows = [
        {
            "Year": row["Year"],
            "Month": row["Month"],
            "WeekPeriod": row["WeekPeriod"],
            "train_station_code": row["train_station_code"],
            "StationName": row["StationName"],
            "Train_number": row["Train_number"],
            "Planned_Train_Departure_Time": _format_time(row["planned_time"]),
            "PassengersAscending": row["PassengersAscending"],
            "PassengersDescending": row["PassengersDescending"],
        }
        for row in dep_qs.values(
            "Year",
            "Month",
            "WeekPeriod",
            "train_station_code",
            "StationName",
            "Train_number",
            "planned_time",
            "PassengersAscending",
            "PassengersDescending",
        )
    ]

    context = {
        "debug_message": debug_message,
        "station": station or "",
        "year": year or "",
        "month": month or "",
        "station_options": station_options,
        "year_options": year_options,
        "month_options": month_options,
        "arr_rows": arr_rows,
        "dep_rows": dep_rows,
    }
    return render(request, "train_times.html", context)


def train_number(request):
    train_number = (request.GET.get("train_number") or "").strip()
    station = (request.GET.get("station") or "").strip()
    year = (request.GET.get("year") or "").strip()
    month = (request.GET.get("month") or "").strip()

    return render(
        request,
        "train_number.html",
        {
            "train_number": train_number,
            "station": station,
            "year": year,
            "month": month,
        },
    )
