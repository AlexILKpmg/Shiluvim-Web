from django.db import models


class ConvergenceRecord(models.Model):
    year = models.IntegerField()
    month = models.IntegerField()
    week_period = models.CharField(max_length=50)
    train_station_name = models.CharField(max_length=255)
    rail_direction = models.CharField(max_length=255)
    train_number = models.CharField(max_length=50)
    observations_count = models.IntegerField()
    on_time_count = models.IntegerField()
    on_time_percentage = models.DecimalField(max_digits=5, decimal_places=2)

    def __str__(self):
        return f"{self.train_station_name} - {self.train_number} ({self.month}/{self.year})"
