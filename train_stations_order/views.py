from django.shortcuts import render

from train_stations_order.models import Ranking


def stations_order(request):
    train_number = (request.GET.get("train_number") or "").strip()
    station = (request.GET.get("station") or "").strip()
    year = (request.GET.get("year") or "").strip()
    month = (request.GET.get("month") or "").strip()

    rows = []
    debug_message = ""

    if train_number:
        try:
            try:
                train_num = int(train_number)
            except ValueError:
                train_num = None

            if train_num is None:
                debug_message = "train_number must be numeric."
            else:
                qs = (
                    Ranking.objects.filter(train_num=train_num)
                    .order_by("train_station_order", "id")
                    .values("train_station_name", "train_station_order")
                )
                rows = [
                    {
                        "StationName": row["train_station_name"],
                        "train_rishui_station_order_source": row["train_station_order"],
                    }
                    for row in qs
                ]
        except Exception as exc:
            debug_message = f"Failed reading data from DB: {exc}"

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
