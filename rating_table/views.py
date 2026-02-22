# rating_table/views.py
from pathlib import Path
import pandas as pd
from django.shortcuts import render

TABLES_DIR = Path(r"C:\Users\ailyaev\Desktop\פרויקטים\Shiluvim Web") / "tables"


def main_page(request, station_name=None, year=None, month=None):
    # ------------------------------------------------------------
    # 1) Get filters (URL path params first, fallback to query params)
    # ------------------------------------------------------------
    if station_name is None:
        station_name = request.GET.get("station_name", "").strip()

    if year is None:
        y = request.GET.get("year", "").strip()
        year = int(y) if y else None

    if month is None:
        m = request.GET.get("month", "").strip()
        month = int(m) if m else None

    # ------------------------------------------------------------
    # 2) Load files
    # ------------------------------------------------------------
    df_ranking = pd.read_csv(TABLES_DIR / "df_ranking.csv", encoding="utf-8-sig")
    passanger_matrix = pd.read_csv(TABLES_DIR / "passanger_matrix_10_11_2025.csv", encoding="utf-8-sig")

    bus_info = pd.read_excel(TABLES_DIR / "for_convergence_data.xlsx")
    bus_info = bus_info.rename(columns={
        "שם תחנת הרכבת": "station_name",
        "מפעיל": "operator",
        "קוד תחנת אוטובוס": "bus_code_name",
        "שם תחנת אוטובוס": "bus_station_name",
        "מק\"ט": "OfficeLineID",
        "שילוט": "Line",
        "כיוון": "Direction",
        "חלופה": "Alternative",
        "סוג קו": "LineType",
        "מוצא": "Start_StopCode",
        "יעד": "End_StopCode",
        "תקופת שבוע": "WeekPeriod",
        "כיוון נסיעת האוטובוס": "Bus_Direction",
    })

    # ------------------------------------------------------------
    # 3) Filter by station/year/month
    # ------------------------------------------------------------
    if station_name:
        df_ranking = df_ranking[df_ranking["StationName"] == station_name]
        passanger_matrix = passanger_matrix[passanger_matrix["FromStationName"] == station_name]
        bus_info = bus_info[bus_info["station_name"] == station_name]

    if year is not None:
        df_ranking = df_ranking[df_ranking["Year"] == year]
        passanger_matrix = passanger_matrix[passanger_matrix["Year"] == year]

    if month is not None:
        df_ranking = df_ranking[df_ranking["Month"] == month]
        passanger_matrix = passanger_matrix[passanger_matrix["Month"] == month]

    # ------------------------------------------------------------
    # 4) Build MATRIX (pivot) for template
    #    rows   = FromStationName
    #    cols   = ToStationName
    #    values = SUM_Values_Pass
    # ------------------------------------------------------------
    if passanger_matrix.empty:
        matrix_cols = []
        matrix_rows = []
    else:
        pivot = passanger_matrix.pivot_table(
            index="FromStationName",
            columns="ToStationName",
            values="SUM_Values_Pass",
            aggfunc="sum",
            fill_value=0
        )

        matrix_cols = [str(c) for c in pivot.columns.tolist()]

        matrix_rows = []
        for from_station, row in pivot.iterrows():
            matrix_rows.append({
                "FromStationName": str(from_station),
                "values": [int(v) if pd.notna(v) else 0 for v in row.tolist()],
            })

    # ------------------------------------------------------------
    # 5) Context
    # ------------------------------------------------------------
    context = {
        "station_name": station_name or "",
        "year": year or "",
        "month": month or "",

        "df_ranking": df_ranking.to_dict(orient="records"),

        # NEW: matrix-friendly payload
        "matrix_cols": matrix_cols,
        "matrix_rows": matrix_rows,

        "bus_info": bus_info.to_dict(orient="records"),
    }

    return render(request, "main_page.html", context)
