# <third_app_name>/models.py
from django.db import models


class ConvergenceTable(models.Model):
    train_station_name = models.CharField(max_length=255)
    operator = models.CharField(max_length=255)
    bus_code_name = models.IntegerField()
    bus_station_name = models.CharField(max_length=255)
    officelineid = models.IntegerField()
    line = models.IntegerField()
    direction = models.IntegerField()
    alternative = models.CharField(max_length=255)
    line_type = models.CharField(max_length=255)
    start_stopcode = models.CharField(max_length=255)
    end_stopcode = models.CharField(max_length=255)
    week_period = models.CharField(max_length=255)
    bus_direction = models.CharField(max_length=255)

    class Meta:
        managed = False  # CSV (no DB table)
