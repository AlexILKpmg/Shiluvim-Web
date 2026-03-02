from pathlib import Path

import pandas as pd
from django.shortcuts import render


TABLES_DIR = Path(__file__).resolve().parent.parent / "tables"


def stations_order(request):
    train_number = (request.GET.get("train_number") or "").strip()
    station = (request.GET.get("station") or "").strip()
    year = (request.GET.get("year") or "").strip()
    month = (request.GET.get("month") or "").strip()

    rows = []
    debug_message = ""

    if train_number:
        xlsx_path = TABLES_DIR / "Train_Stations_Order.xlsx"
        try:
            df = pd.read_excel(xlsx_path)
            if "Train_number" in df.columns:
                df["Train_number"] = df["Train_number"].astype(str).str.strip()
                filtered = df[df["Train_number"] == train_number]
                if "train_rishui_station_order_source" in filtered.columns:
                    filtered["train_rishui_station_order_source"] = pd.to_numeric(
                        filtered["train_rishui_station_order_source"], errors="coerce"
                    )
                    filtered = filtered.sort_values(
                        by="train_rishui_station_order_source",
                        ascending=True,
                        na_position="last",
                    )
                rows = filtered.to_dict(orient="records")
            else:
                debug_message = "Column 'Train_number' was not found in Train_Stations_Order.xlsx."
        except Exception as exc:
            debug_message = f"Failed reading Train_Stations_Order.xlsx: {exc}"

    return render(
        request,
        "train_station_order.html",
        {
            "train_number": train_number,
            "station": station,
            "year": year,
            "month": month,
            "rows": rows,
            "debug_message": debug_message,
        },
    )
