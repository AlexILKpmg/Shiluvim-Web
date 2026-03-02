# <matrix passengers>/models.py
from django.db import models


class PassengerMatrix(models.Model):
    from_station_name = models.CharField(max_length=255)
    to_station_name = models.CharField(max_length=255)
    month = models.IntegerField()
    year = models.IntegerField()
    sum_values_pass = models.IntegerField()

    def __str__(self):
        return f"{self.from_station_name} -> {self.to_station_name} ({self.month}/{self.year})"
