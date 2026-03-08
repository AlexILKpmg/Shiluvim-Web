from django.db import models


class ConvergenceBusToRail(models.Model):
    year = models.IntegerField()
    month = models.IntegerField()
    week_period = models.CharField(max_length=50)
    train_station_name = models.CharField(max_length=255)
    rail_direction = models.CharField(max_length=255)
    train_number = models.IntegerField()
    signage = models.IntegerField()
    is_gold_train = models.CharField(max_length=10, blank=True)
    is_bus_on_time = models.IntegerField(null=True, blank=True)
    rishui_train_departure_time = models.CharField(max_length=32, blank=True)

    operator = models.CharField(max_length=255)
    makat = models.IntegerField(null=True, blank=True)
    direction = models.IntegerField(null=True, blank=True)
    alternative = models.CharField(max_length=255, blank=True)
    departure_time = models.CharField(max_length=32, blank=True)
    avg_passengers_per_trip = models.FloatField(null=True, blank=True)

    arrival_time_to_station = models.CharField(max_length=32, blank=True)
    arrival_time_window = models.CharField(max_length=255, blank=True)
    minutes_gap_bus_to_rail = models.FloatField(null=True, blank=True)
    recommended_minutes = models.IntegerField(null=True, blank=True)

    observations_count = models.IntegerField(null=True, blank=True)
    on_time_count = models.IntegerField(null=True, blank=True)
    on_time_percentage = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=(
                    "year",
                    "month",
                    "week_period",
                    "train_station_name",
                    "rail_direction",
                    "train_number",
                    "makat",
                    "departure_time",
                ),
                name="uniq_cov_b2r_row",
            ),
        ]

    def __str__(self):
        return (
            f"B2R {self.train_station_name} #{self.train_number} "
            f"({self.month}/{self.year}, {self.week_period})"
        )


class ConvergenceRailToBus(models.Model):
    year = models.IntegerField()
    month = models.IntegerField()
    week_period = models.CharField(max_length=50)
    train_station_name = models.CharField(max_length=255)
    rail_direction = models.CharField(max_length=255)
    train_number = models.IntegerField()
    signage = models.IntegerField()
    is_gold_train = models.CharField(max_length=10, blank=True)
    is_bus_on_time = models.IntegerField(null=True, blank=True)
    rishui_train_arrival_time = models.CharField(max_length=32, blank=True)

    operator = models.CharField(max_length=255)
    makat = models.IntegerField(null=True, blank=True)
    direction = models.IntegerField(null=True, blank=True)
    alternative = models.CharField(max_length=255, blank=True)
    departure_time = models.CharField(max_length=32, blank=True)
    avg_passengers_per_trip = models.FloatField(null=True, blank=True)

    train_departure_time = models.CharField(max_length=32, blank=True)
    minutes_gap_rail_to_bus = models.FloatField(null=True, blank=True)
    recommended_minutes = models.IntegerField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=(
                    "year",
                    "month",
                    "week_period",
                    "train_station_name",
                    "rail_direction",
                    "train_number",
                    "makat",
                    "departure_time",
                ),
                name="uniq_cov_r2b_row",
            ),
        ]

    def __str__(self):
        return (
            f"R2B {self.train_station_name} #{self.train_number} "
            f"({self.month}/{self.year}, {self.week_period})"
        )
