import tempfile
from pathlib import Path

import pandas as pd
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from train_stations_order.models import Ranking


class ImportTrainStationsOrderCommandTests(TestCase):
    def _write_csv(self, content: str) -> Path:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8", newline="")
        try:
            tmp.write(content)
            path = Path(tmp.name)
            self.addCleanup(path.unlink, missing_ok=True)
            return path
        finally:
            tmp.close()

    def _write_xlsx(self, rows) -> Path:
        tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        try:
            path = Path(tmp.name)
            pd.DataFrame(rows).to_excel(path, index=False)
            self.addCleanup(path.unlink, missing_ok=True)
            return path
        finally:
            tmp.close()

    def test_import_canonical_csv_inserts(self):
        csv_path = self._write_csv(
            "train_num,train_station_id,train_station_order,train_station_name\n"
            "7,1400,1,Tel Aviv\n"
        )

        call_command("import_train_stations_order", "--file", str(csv_path))

        self.assertTrue(Ranking.objects.filter(train_num=7, train_station_id=1400, train_station_order=1).exists())

    def test_import_excel_aliases_inserts(self):
        xlsx_path = self._write_xlsx(
            [
                {
                    "Train_number": 8,
                    "train_station_code": 3100,
                    "train_rishui_station_order_source": 2,
                    "StationName": "Binyamina",
                    "WeekPeriod": "Weekday",
                }
            ]
        )

        call_command("import_train_stations_order", "--file", str(xlsx_path))

        self.assertTrue(Ranking.objects.filter(train_num=8, train_station_id=3100, train_station_order=2).exists())

    def test_duplicate_rows_are_skipped(self):
        Ranking.objects.create(train_num=9, train_station_id=3200, train_station_order=3, train_station_name="Haifa")
        csv_path = self._write_csv(
            "train_num,train_station_id,train_station_order,train_station_name\n"
            "9,3200,3,Haifa\n"
        )

        call_command("import_train_stations_order", "--file", str(csv_path))

        self.assertEqual(Ranking.objects.filter(train_num=9, train_station_id=3200, train_station_order=3).count(), 1)

    def test_dry_run_does_not_write(self):
        csv_path = self._write_csv(
            "train_num,train_station_id,train_station_order,train_station_name\n"
            "10,3300,4,Ashkelon\n"
        )

        call_command("import_train_stations_order", "--file", str(csv_path), "--dry-run")

        self.assertFalse(Ranking.objects.filter(train_num=10).exists())

    def test_missing_required_columns_fails(self):
        csv_path = self._write_csv(
            "train_num,train_station_id,train_station_name\n"
            "11,3400,Eilat\n"
        )

        with self.assertRaises(CommandError):
            call_command("import_train_stations_order", "--file", str(csv_path))
