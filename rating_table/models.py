# ranking talbe/models.py
from django.db import models


class Ranking(models.Model):
    year = models.IntegerField()
    month = models.IntegerField()
    train_station_name = models.CharField(max_length=255)
    ascending_pass = models.IntegerField()
    descending_pass = models.IntegerField()
    rank = models.CharField(max_length=255)

    class Meta: #adding constains that i can have only one value for each "group"
        constraints = [
            models.UniqueConstraint(fields=["year", "month", "train_station_name"], name="uniq_ranking_month_station")
        ]

    def __str__(self):
        return f"{self.train_station_name} {self.month}/{self.year}"
