from collections import defaultdict

from django.shortcuts import render

from bus_info_per_train_station_table.models import BusInfo
from matrix_pass_table.models import PassengerMatrix
from rating_table.models import Ranking


def main_page(request):
    station_name = request.GET.get("station_name", "").strip()

    y = request.GET.get("year", "").strip()
    year = int(y) if y else None

    m = request.GET.get("month", "").strip()
    month = int(m) if m else None

    ranking_qs = Ranking.objects.all()
    matrix_qs = PassengerMatrix.objects.all()
    bus_qs = BusInfo.objects.all()

    if station_name:
        ranking_qs = ranking_qs.filter(train_station_name=station_name)
        matrix_qs = matrix_qs.filter(from_station_name=station_name)
        bus_qs = bus_qs.filter(train_station_name=station_name)

    if year is not None:
        ranking_qs = ranking_qs.filter(year=year)
        matrix_qs = matrix_qs.filter(year=year)

    if month is not None:
        ranking_qs = ranking_qs.filter(month=month)
        matrix_qs = matrix_qs.filter(month=month)

    df_ranking = [
        {
            "Year": row["year"],
            "Month": row["month"],
            "StationName": row["train_station_name"],
            "Ascending_pass": row["ascending_pass"],
            "Descending_pass": row["descending_pass"],
            "Rank": row["rank"],
        }
        for row in ranking_qs.values(
            "year",
            "month",
            "train_station_name",
            "ascending_pass",
            "descending_pass",
            "rank",
        )
    ]

    matrix_raw = list(matrix_qs.values("from_station_name", "to_station_name", "sum_values_pass"))
    if not matrix_raw:
        matrix_cols = []
        matrix_rows = []
    else:
        cell_map: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        col_set = set()

        for row in matrix_raw:
            from_station = str(row["from_station_name"])
            to_station = str(row["to_station_name"])
            value = int(row["sum_values_pass"] or 0)
            cell_map[from_station][to_station] += value
            col_set.add(to_station)

        matrix_cols = sorted(col_set)
        matrix_rows = [
            {
                "FromStationName": from_station,
                "values": [cell_map[from_station].get(col, 0) for col in matrix_cols],
            }
            for from_station in sorted(cell_map.keys())
        ]

    bus_info = [
        {
            "station_name": row["train_station_name"],
            "operator": row["operator"],
            "bus_code_name": row["bus_code_name"],
            "bus_station_name": row["bus_station_name"],
            "OfficeLineID": row["officelineid"],
            "Line": row["line"],
            "Direction": row["direction"],
            "Alternative": row["alternative"],
            "LineType": row["line_type"],
            "Start_StopCode": row["start_stopcode"],
            "End_StopCode": row["end_stopcode"],
            "WeekPeriod": row["week_period"],
            "Bus_Direction": row["bus_direction"],
        }
        for row in bus_qs.values(
            "train_station_name",
            "operator",
            "bus_code_name",
            "bus_station_name",
            "officelineid",
            "line",
            "direction",
            "alternative",
            "line_type",
            "start_stopcode",
            "end_stopcode",
            "week_period",
            "bus_direction",
        )
    ]

    context = {
        "station_name": station_name or "",
        "year": year or "",
        "month": month or "",
        "df_ranking": df_ranking,
        "matrix_cols": matrix_cols,
        "matrix_rows": matrix_rows,
        "bus_info": bus_info,
    }

    return render(request, "main_page.html", context)
