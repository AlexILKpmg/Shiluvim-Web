from django.shortcuts import render

from convergence.models import OverrideConv


def history_page(request):
    station = (request.GET.get("station") or "").strip()

    overrides = OverrideConv.objects.all()

    if station:
        overrides = overrides.filter(station_name=station)

    overrides = overrides.order_by("-changed_at")

    return render(
        request,
        "history.html",
        {
            "overrides": overrides,
            "station": station,
        },
    )