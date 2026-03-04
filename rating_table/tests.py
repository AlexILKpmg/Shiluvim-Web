import tempfile
from pathlib import Path

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from rating_table.models import Ranking


class ImportRatingTableCommandTests(TestCase):
    def _write_csv(self, content: str) -> Path:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8", newline="")
        try:
            tmp.write(content)
            path = Path(tmp.name)
            self.addCleanup(path.unlink, missing_ok=True)
            return path
        finally:
            tmp.close()

    def test_import_inserts_rows(self):
        csv_path = self._write_csv(
            "year,month,train_station_name,ascending_pass,descending_pass,rank\n"
            "2026,3,Tel Aviv,100,120,A\n"
        )

        call_command("import_rating_table", "--file", str(csv_path))

        row = Ranking.objects.get(year=2026, month=3, train_station_name="Tel Aviv")
        self.assertEqual(row.ascending_pass, 100)
        self.assertEqual(row.descending_pass, 120)
        self.assertEqual(row.rank, "A")

    def test_import_updates_existing_row(self):
        Ranking.objects.create(
            year=2026,
            month=3,
            train_station_name="Jerusalem",
            ascending_pass=10,
            descending_pass=20,
            rank="B",
        )
        csv_path = self._write_csv(
            "year,month,train_station_name,ascending_pass,descending_pass,rank\n"
            "2026,3,Jerusalem,33,44,A+\n"
        )

        call_command("import_rating_table", "--file", str(csv_path))

        row = Ranking.objects.get(year=2026, month=3, train_station_name="Jerusalem")
        self.assertEqual(row.ascending_pass, 33)
        self.assertEqual(row.descending_pass, 44)
        self.assertEqual(row.rank, "A+")

    def test_dry_run_does_not_write(self):
        csv_path = self._write_csv(
            "year,month,train_station_name,ascending_pass,descending_pass,rank\n"
            "2026,3,Haifa,50,60,B\n"
        )

        call_command("import_rating_table", "--file", str(csv_path), "--dry-run")

        self.assertFalse(Ranking.objects.filter(year=2026, month=3, train_station_name="Haifa").exists())

    def test_strict_mode_rolls_back_all_writes(self):
        csv_path = self._write_csv(
            "year,month,train_station_name,ascending_pass,descending_pass,rank\n"
            "2026,3,Ashdod,5,6,C\n"
            "2026,13,Eilat,1,1,D\n"
        )

        with self.assertRaises(CommandError):
            call_command("import_rating_table", "--file", str(csv_path), "--strict")

        self.assertFalse(Ranking.objects.filter(train_station_name="Ashdod").exists())

    def test_invalid_header_fails(self):
        csv_path = self._write_csv(
            "year,month,station_name,ascending_pass,descending_pass,rank\n"
            "2026,3,Beer Sheva,10,20,B\n"
        )

        with self.assertRaises(CommandError):
            call_command("import_rating_table", "--file", str(csv_path))
