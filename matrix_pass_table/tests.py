import tempfile
from pathlib import Path

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from matrix_pass_table.models import PassengerMatrix


class ImportMatrixPassTableCommandTests(TestCase):
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
            "from_station_name,to_station_name,month,year,sum_values_pass\n"
            "Tel Aviv,Haifa,3,2026,100\n"
        )

        call_command("import_matrix_pass_table", "--file", str(csv_path))

        row = PassengerMatrix.objects.get(
            from_station_name="Tel Aviv",
            to_station_name="Haifa",
            month=3,
            year=2026,
        )
        self.assertEqual(row.sum_values_pass, 100)

    def test_import_updates_existing_row(self):
        PassengerMatrix.objects.create(
            from_station_name="Jerusalem",
            to_station_name="Beer Sheva",
            month=3,
            year=2026,
            sum_values_pass=10,
        )
        csv_path = self._write_csv(
            "from_station_name,to_station_name,month,year,sum_values_pass\n"
            "Jerusalem,Beer Sheva,3,2026,33\n"
        )

        call_command("import_matrix_pass_table", "--file", str(csv_path))

        row = PassengerMatrix.objects.get(
            from_station_name="Jerusalem",
            to_station_name="Beer Sheva",
            month=3,
            year=2026,
        )
        self.assertEqual(row.sum_values_pass, 33)

    def test_dry_run_does_not_write(self):
        csv_path = self._write_csv(
            "from_station_name,to_station_name,month,year,sum_values_pass\n"
            "Ashdod,Eilat,3,2026,50\n"
        )

        call_command("import_matrix_pass_table", "--file", str(csv_path), "--dry-run")

        self.assertFalse(
            PassengerMatrix.objects.filter(
                from_station_name="Ashdod",
                to_station_name="Eilat",
                month=3,
                year=2026,
            ).exists()
        )

    def test_strict_mode_rolls_back_all_writes(self):
        csv_path = self._write_csv(
            "from_station_name,to_station_name,month,year,sum_values_pass\n"
            "A,B,3,2026,5\n"
            "X,Y,13,2026,1\n"
        )

        with self.assertRaises(CommandError):
            call_command("import_matrix_pass_table", "--file", str(csv_path), "--strict")

        self.assertFalse(PassengerMatrix.objects.filter(from_station_name="A", to_station_name="B").exists())

    def test_invalid_header_fails(self):
        csv_path = self._write_csv(
            "from_station,to_station,month,year,sum_values_pass\n"
            "Tel Aviv,Haifa,3,2026,100\n"
        )

        with self.assertRaises(CommandError):
            call_command("import_matrix_pass_table", "--file", str(csv_path))
