from django.shortcuts import render

from convergence.models import OverrideConv



def history_page(request):
    overrides = OverrideConv.objects.order_by("-changed_at")

    return render(
        request,
        "history.html",
        {"overrides": overrides},
    )