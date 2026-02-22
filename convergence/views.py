import json
from pathlib import Path

import pandas as pd
from django.shortcuts import render


TABLES_DIR = Path(__file__).resolve().parents[1] / "tables"

CONVERGENCE_FILES = [
    ("WeekDay_rail_bus_convergence_2025-10.xlsx", "יום חול"),
    ("WeekDay_rail_bus_convergence_2025-11.xlsx", "יום חול"),
    ("Friday_rail_bus_convergence_2025-10.xlsx", "שישי"),
    ("Friday_rail_bus_convergence_2025-11.xlsx", "שישי"),
    ("Saturday_rail_bus_convergence_2025-10.xlsx", "שבת"),
    ("Saturday_rail_bus_convergence_2025-11.xlsx", "שבת"),
]

COL_STATION = "שם תחנת הרכבת"
COL_YEAR = "שנה"
COL_MONTH = "חודש"
COL_WEEK = "תקופת שבוע"
COL_RAIL_DIR = "כיוון נסיעת הרכבת"
COL_TRAIN_ID = "מספר הרכבת"
COL_PERC = "אחוז הנסיעות שעמדו בזמנים"

DIR_TO_TA = "לכיוון תל אביב"
DIR_FROM_TA = "מכיוון תל אביב"


def _to_json_records(df: pd.DataFrame) -> str:
    if df.empty:
        return "[]"
    clean = df.where(pd.notna(df), None)
    return clean.to_json(orient="records", force_ascii=False)


def _read_convergence_tables() -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    errors: list[str] = []

    for file_name, week_label in CONVERGENCE_FILES:
        path = TABLES_DIR / file_name
        try:
            df = pd.read_excel(path)
            df[COL_WEEK] = week_label
            frames.append(df)
        except Exception as e:
            errors.append(f"בעיה בקריאת קובץ {file_name}: {e}")

    if not frames:
        return pd.DataFrame(), errors
    return pd.concat(frames, ignore_index=True), errors


def _build_train_perc_map(df: pd.DataFrame) -> dict[str, object]:
    required = [COL_YEAR, COL_MONTH, COL_TRAIN_ID, COL_RAIL_DIR, COL_PERC]
    if any(c not in df.columns for c in required):
        return {}

    out: dict[str, object] = {}
    tmp = df[required].copy()

    tmp[COL_YEAR] = pd.to_numeric(tmp[COL_YEAR], errors="coerce").astype("Int64")
    tmp[COL_MONTH] = pd.to_numeric(tmp[COL_MONTH], errors="coerce").astype("Int64")
    tmp[COL_TRAIN_ID] = tmp[COL_TRAIN_ID].astype(str).str.strip()
    tmp[COL_RAIL_DIR] = tmp[COL_RAIL_DIR].astype(str).str.strip()

    tmp = tmp.dropna(subset=[COL_YEAR, COL_MONTH, COL_PERC])
    tmp = tmp[tmp[COL_TRAIN_ID] != ""]
    tmp = tmp[tmp[COL_RAIL_DIR] != ""]

    for _, row in tmp.iterrows():
        y = int(row[COL_YEAR])
        m = int(row[COL_MONTH])
        t = row[COL_TRAIN_ID]
        d = row[COL_RAIL_DIR]
        perc = row[COL_PERC]

        key = f"{y}_{m}_{t}_{d}"
        out[key] = perc

    return out


def convergence(request):
    station = (request.GET.get("station") or "").strip()

    y = (request.GET.get("year") or "").strip()
    year = int(y) if y.isdigit() else None

    m = (request.GET.get("month") or "").strip()
    month = int(m) if m.isdigit() else None

    # Same guard behavior as train_times: require all three filters from URL.
    if not station or year is None or month is None:
        return render(
            request,
            "convergence.html",
            {
                "debug_message": "חסר station/year/month ב-URL, לכן לא מוצגים נתונים.",
                "station": station or "",
                "year": year or "",
                "month": month or "",
                "bus_to_rail_df_js": "[]",
                "rail_to_bus_df_js": "[]",
                "train_perc_js": "{}",
            },
        )

    all_df, errors = _read_convergence_tables()
    if all_df.empty:
        return render(
            request,
            "convergence.html",
            {
                "debug_message": "\n".join(errors) if errors else "לא נמצאו נתונים.",
                "station": station,
                "year": year,
                "month": month,
                "bus_to_rail_df_js": "[]",
                "rail_to_bus_df_js": "[]",
                "train_perc_js": "{}",
            },
        )

    filtered = all_df.copy()

    if COL_STATION in filtered.columns:
        filtered = filtered[filtered[COL_STATION].astype(str).str.strip() == station]
    if COL_YEAR in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered[COL_YEAR], errors="coerce") == year]
    if COL_MONTH in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered[COL_MONTH], errors="coerce") == month]

    if COL_RAIL_DIR in filtered.columns:
        bus_to_rail_df = filtered[filtered[COL_RAIL_DIR].astype(str).str.strip() == DIR_TO_TA]
        rail_to_bus_df = filtered[filtered[COL_RAIL_DIR].astype(str).str.strip() == DIR_FROM_TA]
    else:
        bus_to_rail_df = filtered.iloc[0:0]
        rail_to_bus_df = filtered.iloc[0:0]

    train_perc_map = _build_train_perc_map(filtered)

    context = {
        "debug_message": "\n".join(errors),
        "station": station,
        "year": year,
        "month": month,
        "bus_to_rail_df_js": _to_json_records(bus_to_rail_df),
        "rail_to_bus_df_js": _to_json_records(rail_to_bus_df),
        "train_perc_js": json.dumps(train_perc_map, ensure_ascii=False),
    }
    return render(request, "convergence.html", context)
