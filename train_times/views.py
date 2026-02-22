from pathlib import Path
import pandas as pd
from django.shortcuts import render


TABLES_DIR = Path(r"C:\Users\ailyaev\Desktop\פרויקטים\Shiluvim Web") / "tables"

DEP_RENAME = {
    'שנה': 'Year',
    'חודש': 'Month',
    'תקופת שבוע': 'WeekPeriod',
    'קוד תחנת הרכבת': 'train_station_code',
    'שם תחנת הרכבת': 'StationName',
    'מספר רכבת': 'Train_number',
    'זמן יציאה מהתחנה (רישוי)': 'Planned_Train_Departure_Time',
    'מספר עולים בתחנה': 'PassengersAscending',
    'מספר יורדים בתחנה': 'PassengersDescending',
}

ARR_RENAME = {
    'שנה': 'Year',
    'חודש': 'Month',
    'תקופת שבוע': 'WeekPeriod',
    'קוד תחנת הרכבת': 'train_station_code',
    'שם תחנת הרכבת': 'StationName',
    'מספר רכבת': 'Train_number',
    'זמן הגעה לתחנה (רישוי)': 'Planned_Train_Arrivel_Time',
    'מספר עולים בתחנה': 'PassengersAscending',
    'מספר יורדים בתחנה': 'PassengersDescending',
}


def train_times(request):
    # ------------------------------------------------------------
    # 1) Read query params (server filters)
    # ------------------------------------------------------------
    station = (request.GET.get("station") or "").strip()

    y = (request.GET.get("year") or "").strip()
    year = int(y) if y.isdigit() else None

    m = (request.GET.get("month") or "").strip()
    month = int(m) if m.isdigit() else None

    debug_message = ""

    # ------------------------------------------------------------
    # GUARD: don't expose full tables unless filters are provided
    # ------------------------------------------------------------
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

    # ------------------------------------------------------------
    # 2) Load CSVs (use utf-8-sig like main_page)
    # ------------------------------------------------------------
    arr_path = TABLES_DIR / "Arrivel_train_passengers_numbers.csv"
    dep_path = TABLES_DIR / "Departure_train_passengers_numbers.csv"

    try:
        arr_df = pd.read_csv(arr_path, encoding="utf-8-sig")
    except Exception as e:
        arr_df = pd.DataFrame()
        debug_message += f"בעיה בקריאת קובץ הגעה: {e}\n"

    try:
        dep_df = pd.read_csv(dep_path, encoding="utf-8-sig")
    except Exception as e:
        dep_df = pd.DataFrame()
        debug_message += f"בעיה בקריאת קובץ יציאה: {e}\n"

    # ------------------------------------------------------------
    # 3) Rename columns to CONSISTENT English keys (so template is stable)
    #    This supports your case where CSV headers are Hebrew.
    # ------------------------------------------------------------
    if not arr_df.empty:
        arr_df = arr_df.rename(columns=ARR_RENAME)
    if not dep_df.empty:
        dep_df = dep_df.rename(columns=DEP_RENAME)

    # ------------------------------------------------------------
    # 4) Build dropdown options BEFORE filtering (so you can choose station/year/month)
    # ------------------------------------------------------------
    # Stations (union of both tables)
    station_options = sorted(set(
        pd.concat([
            arr_df.get("StationName", pd.Series(dtype=str)),
            dep_df.get("StationName", pd.Series(dtype=str)),
        ], ignore_index=True).dropna().astype(str).str.strip().tolist()
    ))

    # years/months available for selected station
    def df_for_station(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "StationName" not in df.columns:
            return df.iloc[0:0]
        if not station:
            return df
        return df[df["StationName"].astype(str).str.strip() == station]

    arr_s = df_for_station(arr_df)
    dep_s = df_for_station(dep_df)
    combined_s = pd.concat([arr_s, dep_s], ignore_index=True) if (not arr_s.empty or not dep_s.empty) else pd.DataFrame()

    if not combined_s.empty and "Year" in combined_s.columns:
        year_options = sorted(pd.to_numeric(combined_s["Year"], errors="coerce").dropna().astype(int).unique().tolist())
    else:
        year_options = []

    # If user gave a year that doesn't exist -> set None (so month_options doesn't break)
    if year is not None and year not in year_options:
        year = None

    if (year is not None) and (not combined_s.empty) and ("Month" in combined_s.columns) and ("Year" in combined_s.columns):
        months_series = combined_s[pd.to_numeric(combined_s["Year"], errors="coerce") == year]["Month"]
        month_options = sorted(pd.to_numeric(months_series, errors="coerce").dropna().astype(int).unique().tolist())
    else:
        month_options = []

    if month is not None and month not in month_options:
        month = None

    # ------------------------------------------------------------
    # 5) Server slice (station/year/month) — EXACTLY like main_page
    #    Note: We do NOT slice by WeekPeriod here (client-side only)
    # ------------------------------------------------------------
    def slice_df(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        out = df.copy()

        if station and "StationName" in out.columns:
            out = out[out["StationName"].astype(str).str.strip() == station]
        if year is not None and "Year" in out.columns:
            out = out[pd.to_numeric(out["Year"], errors="coerce") == year]
        if month is not None and "Month" in out.columns:
            out = out[pd.to_numeric(out["Month"], errors="coerce") == month]

        return out

    arr_f = slice_df(arr_df)
    dep_f = slice_df(dep_df)


    # ------------------------------------------------------------
    # 6) Send records to template (NO to_html)
    # ------------------------------------------------------------
    context = {
        "debug_message": debug_message,

        "station": station or "",
        "year": year or "",
        "month": month or "",

        "station_options": station_options,
        "year_options": year_options,
        "month_options": month_options,

        "arr_rows": arr_f.to_dict(orient="records"),
        "dep_rows": dep_f.to_dict(orient="records"),
    }
    return render(request, "train_times.html", context)
