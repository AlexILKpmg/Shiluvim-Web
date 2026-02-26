import json
import re
from pathlib import Path

import pandas as pd
from django.shortcuts import render


TABLES_DIR = Path(__file__).resolve().parents[1] / "tables"

YEAR_MONTHS = {"2025-12", "2026-01"}  # update only this; empty set => all months
WEEK_LABELS = {
    "WeekDay": "יום חול",
    "Friday": "שישי",
    "Saturday": "שבת",
}
CONVERGENCE_FILE_RE = re.compile(
    r"^(WeekDay|Friday|Saturday)_rail_bus_convergence_(\d{4}-\d{2})\.xlsx$",
    re.IGNORECASE,
)

COL_STATION = "שם תחנת הרכבת"
COL_YEAR = "שנה"
COL_MONTH = "חודש"
COL_WEEK = "תקופת שבוע"
COL_RAIL_DIR = "כיוון נסיעת הרכבת"
COL_TRAIN_ID = "מספר הרכבת"
COL_PERC = "אחוז הנסיעות שעמדו בזמנים"
COL_N = "מספר תצפיות"
COL_N_postive_flagged = "מספר הנסיעות שעמדו בזמנים"

DIR_TO_TA = "לכיוון תל אביב"
DIR_FROM_TA = "מכיוון תל אביב"


def _to_json_records(df: pd.DataFrame) -> str:
    if df.empty:
        return "[]"
    clean = df.where(pd.notna(df), None)
    return clean.to_json(orient="records", force_ascii=False)

def _discover_convergence_files() -> list[tuple[str, str]]:
    files: list[tuple[str, str, str]] = []  # (year_month, file_name, week_label)
    for p in TABLES_DIR.glob("*.xlsx"):
        match = CONVERGENCE_FILE_RE.match(p.name)
        if not match:
            continue
        week_key, year_month = match.group(1), match.group(2)
        week_key = week_key[0].upper() + week_key[1:]  # normalize case
        if YEAR_MONTHS and year_month not in YEAR_MONTHS:
            continue
        files.append((year_month, p.name, WEEK_LABELS[week_key]))
    files.sort(key=lambda item: (item[0], item[1]))
    return [(file_name, week_label) for _, file_name, week_label in files]

def _read_convergence_tables() -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    errors: list[str] = []

    for file_name, week_label in _discover_convergence_files():
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
    required = [COL_YEAR, COL_MONTH, COL_TRAIN_ID, COL_RAIL_DIR, COL_N, COL_N_postive_flagged]
    if any(c not in df.columns for c in required):
        return {}

    out: dict[str, object] = {}
    tmp = df[required].copy()

    tmp[COL_YEAR] = pd.to_numeric(tmp[COL_YEAR], errors="coerce").astype("Int64")
    tmp[COL_MONTH] = pd.to_numeric(tmp[COL_MONTH], errors="coerce").astype("Int64")
    tmp[COL_N] = pd.to_numeric(tmp[COL_N], errors="coerce")
    tmp[COL_N_postive_flagged] = pd.to_numeric(tmp[COL_N_postive_flagged], errors="coerce")
    tmp[COL_TRAIN_ID] = tmp[COL_TRAIN_ID].astype(str).str.strip()
    tmp[COL_RAIL_DIR] = tmp[COL_RAIL_DIR].astype(str).str.strip()

    tmp = tmp.dropna(subset=[COL_YEAR, COL_MONTH, COL_N, COL_N_postive_flagged])
    tmp = tmp[tmp[COL_TRAIN_ID] != ""]
    tmp = tmp[tmp[COL_RAIL_DIR] != ""]
    tmp = tmp[tmp[COL_N] > 0]

    grouped = (
        tmp.groupby([COL_YEAR, COL_MONTH, COL_TRAIN_ID, COL_RAIL_DIR], as_index=False)[
            [COL_N_postive_flagged, COL_N]
        ]
        .sum()
    )
    grouped["perc"] = ((grouped[COL_N_postive_flagged] / grouped[COL_N]) * 100).round(1).astype(str) + "%"

    for _, row in grouped.iterrows():
        y = int(row[COL_YEAR])
        m = int(row[COL_MONTH])
        t = row[COL_TRAIN_ID]
        d = row[COL_RAIL_DIR]
        perc = row["perc"]

        key = f"{y}_{m}_{t}_{d}"
        out[key] = perc

    return out


def convergence(request):
    station = (request.GET.get("station") or "").strip()

    y = (request.GET.get("year") or "").strip()
    year = int(y) if y.isdigit() else None

    m = (request.GET.get("month") or "").strip()
    month = int(m) if m.isdigit() else None

    # Require station from URL; year/month can be changed from convergence page.
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

    all_df, errors = _read_convergence_tables()
    if all_df.empty:
        return render(
            request,
            "convergence.html",
            {
                "debug_message": "\n".join(errors) if errors else "no data found",
                "station": station,
                "year": year or "",
                "month": month or "",
                "bus_to_rail_df_js": "[]",
                "rail_to_bus_df_js": "[]",
                "train_perc_js": "{}",
                "year_month_pairs_js": "[]",
            },
        )

    station_df = all_df.copy()
    if COL_STATION in station_df.columns:
        station_df = station_df[station_df[COL_STATION].astype(str).str.strip() == station]

    year_month_pairs: list[dict[str, int]] = []
    if COL_YEAR in station_df.columns and COL_MONTH in station_df.columns:
        ym = station_df[[COL_YEAR, COL_MONTH]].copy()
        ym[COL_YEAR] = pd.to_numeric(ym[COL_YEAR], errors="coerce").astype("Int64")
        ym[COL_MONTH] = pd.to_numeric(ym[COL_MONTH], errors="coerce").astype("Int64")
        ym = ym.dropna(subset=[COL_YEAR, COL_MONTH]).drop_duplicates().sort_values([COL_YEAR, COL_MONTH])
        year_month_pairs = [
            {"year": int(r[COL_YEAR]), "month": int(r[COL_MONTH])}
            for _, r in ym.iterrows()
        ]

    if (year is None or month is None) and year_month_pairs:
        if year is None:
            year = year_month_pairs[0]["year"]
        if month is None:
            month = year_month_pairs[0]["month"]

    filtered = station_df.copy()
    if COL_YEAR in filtered.columns and year is not None:
        filtered = filtered[pd.to_numeric(filtered[COL_YEAR], errors="coerce") == year]
    if COL_MONTH in filtered.columns and month is not None:
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
        "year": year or "",
        "month": month or "",
        "bus_to_rail_df_js": _to_json_records(bus_to_rail_df),
        "rail_to_bus_df_js": _to_json_records(rail_to_bus_df),
        "train_perc_js": json.dumps(train_perc_map, ensure_ascii=False),
        "year_month_pairs_js": json.dumps(year_month_pairs, ensure_ascii=False),
    }
    return render(request, "convergence.html", context)
