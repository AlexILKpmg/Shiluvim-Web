# ranking talbe/models.py
from django.db import models


class Ranking(models.Model):
    year = models.IntegerField()
    month = models.IntegerField()
    train_station_name = models.CharField(max_length=255)
    ascending_pass = models.IntegerField()
    descending_pass = models.IntegerField()
    rank = models.CharField(max_length=255)

    def __str__(self):
        return f"{self.train_station_name} {self.month}/{self.year}"
