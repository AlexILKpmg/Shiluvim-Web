from django.contrib import admin

from .models import ConvergenceBusToRail, ConvergenceRailToBus


admin.site.register(ConvergenceBusToRail)
admin.site.register(ConvergenceRailToBus)
