# train_stations_order/models.py
from django.db import models


class Ranking(models.Model):
    train_num = models.IntegerField()
    train_station_id = models.IntegerField()
    train_station_order = models.IntegerField()
    train_station_name = models.CharField(max_length=255)

    def __str__(self):
        return f"Train {self.train_num} - {self.train_station_name} ({self.train_station_order})"
