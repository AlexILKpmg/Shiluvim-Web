import tempfile
from datetime import time
from pathlib import Path

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from train_times.models import TrainTime


class ImportTrainTimesCommandTests(TestCase):
    def _write_csv(self, content: str) -> Path:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8", newline="")
        try:
            tmp.write(content)
            path = Path(tmp.name)
            self.addCleanup(path.unlink, missing_ok=True)
            return path
        finally:
            tmp.close()

    def test_import_arrival_inserts(self):
        arrival_csv = self._write_csv(
            "Year,Month,WeekPeriod,train_station_code,StationName,Train_number,Planned_Train_Arrivel_Time,PassengersAscending,PassengersDescending\n"
            "2026,1,Weekday,1400,Tel Aviv,7,04:01:00,10,20\n"
        )

        call_command("import_train_times", "--arrival-file", str(arrival_csv))

        self.assertTrue(
            TrainTime.objects.filter(
                Year=2026,
                Month=1,
                train_station_code=1400,
                StationName="Tel Aviv",
                Train_number=7,
                event_type=TrainTime.EventType.ARRIVAL,
            ).exists()
        )

    def test_import_departure_inserts(self):
        departure_csv = self._write_csv(
            "Year,Month,WeekPeriod,train_station_code,StationName,Train_number,Planned_Train_Departure_Time,PassengersAscending,PassengersDescending\n"
            "2026,1,Weekday,1400,Tel Aviv,7,05:01:00,1,2\n"
        )

        call_command("import_train_times", "--departure-file", str(departure_csv))

        self.assertTrue(
            TrainTime.objects.filter(
                event_type=TrainTime.EventType.DEPARTURE,
                planned_time=time(5, 1, 0),
            ).exists()
        )

    def test_dry_run_does_not_write(self):
        arrival_csv = self._write_csv(
            "Year,Month,WeekPeriod,train_station_code,StationName,Train_number,Planned_Train_Arrivel_Time,PassengersAscending,PassengersDescending\n"
            "2026,1,Weekday,1400,Haifa,8,06:00:00,3,4\n"
        )

        call_command("import_train_times", "--arrival-file", str(arrival_csv), "--dry-run")

        self.assertFalse(TrainTime.objects.filter(StationName="Haifa").exists())

    def test_missing_input_files_fails(self):
        with self.assertRaises(CommandError):
            call_command("import_train_times")

    def test_invalid_arrival_header_fails(self):
        arrival_csv = self._write_csv(
            "Year,Month,WeekPeriod,train_station_code,StationName,Train_number,Planned_Train_Arrival_Time,PassengersAscending,PassengersDescending\n"
            "2026,1,Weekday,1400,Tel Aviv,7,04:01:00,10,20\n"
        )

        with self.assertRaises(CommandError):
            call_command("import_train_times", "--arrival-file", str(arrival_csv))
