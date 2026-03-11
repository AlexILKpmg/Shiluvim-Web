# train_times/models.py
from django.db import models

class TrainTime(models.Model):
    class EventType(models.TextChoices):
        TO_TLV = "to_tlv", "To TLV"
        FROM_TLV = "from_tlv", "From TLV"

    Year = models.IntegerField()
    Month = models.IntegerField()
    WeekPeriod = models.CharField(max_length=20)  # e.g. "יום חול", "שישי", "שבת"
    train_station_code = models.IntegerField()
    StationName = models.CharField(max_length=255)
    Train_number = models.IntegerField()

    event_type = models.CharField(max_length=10, choices=EventType.choices)
    planned_time = models.TimeField()  # store HH:MM:SS

    PassengersAscending = models.IntegerField()
    PassengersDescending = models.IntegerField()


    def __str__(self):
        return f"{self.StationName} #{self.Train_number} {self.event_type} {self.planned_time} ({self.Month}/{self.Year})"
