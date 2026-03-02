# train_stations_order/models.py
from django.db import models


class Ranking(models.Model):
    train_num = models.IntegerField()
    train_station_id = models.IntegerField()
    train_station_order = models.IntegerField()
    train_station_name = models.CharField(max_length=255)
    class Meta:
        managed = False  # CSV (no DB table)