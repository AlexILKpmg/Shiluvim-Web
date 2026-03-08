import tempfile
from pathlib import Path

import pandas as pd
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from bus_info_per_train_station_table.models import ConvergenceTable


class ImportBusInfoPerTrainStationCommandTests(TestCase):
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
            df = pd.DataFrame(rows)
            df.to_excel(path, index=False)
            self.addCleanup(path.unlink, missing_ok=True)
            return path
        finally:
            tmp.close()

    def test_import_inserts_row(self):
        csv_path = self._write_csv(
            "train_station_name,operator,bus_code_name,bus_station_name,officelineid,line,direction,alternative,line_type,start_stopcode,end_stopcode,week_period,bus_direction\n"
            "Tel Aviv,Dan,101,Station A,123,5,1,,City,1000,2000,Weekday,North\n"
        )

        call_command("import_bus_info_per_train_station", "--file", str(csv_path))

        self.assertTrue(ConvergenceTable.objects.filter(train_station_name="Tel Aviv", operator="Dan").exists())

    def test_import_skips_existing_duplicate(self):
        row = {
            "train_station_name": "Jerusalem",
            "operator": "Egged",
            "bus_code_name": 202,
            "bus_station_name": "Station B",
            "officelineid": 55,
            "line": 10,
            "direction": 2,
            "alternative": "",
            "line_type": "Intercity",
            "start_stopcode": "3000",
            "end_stopcode": "4000",
            "week_period": "Friday",
            "bus_direction": "South",
        }
        ConvergenceTable.objects.create(**row)
        csv_path = self._write_csv(
            "train_station_name,operator,bus_code_name,bus_station_name,officelineid,line,direction,alternative,line_type,start_stopcode,end_stopcode,week_period,bus_direction\n"
            "Jerusalem,Egged,202,Station B,55,10,2,,Intercity,3000,4000,Friday,South\n"
        )

        call_command("import_bus_info_per_train_station", "--file", str(csv_path))

        self.assertEqual(ConvergenceTable.objects.filter(train_station_name="Jerusalem").count(), 1)

    def test_dry_run_does_not_write(self):
        csv_path = self._write_csv(
            "train_station_name,operator,bus_code_name,bus_station_name,officelineid,line,direction,alternative,line_type,start_stopcode,end_stopcode,week_period,bus_direction\n"
            "Haifa,Dan,303,Station C,44,8,1,,City,5000,6000,Saturday,West\n"
        )

        call_command("import_bus_info_per_train_station", "--file", str(csv_path), "--dry-run")

        self.assertFalse(ConvergenceTable.objects.filter(train_station_name="Haifa").exists())

    def test_invalid_header_fails(self):
        csv_path = self._write_csv(
            "station_name,operator,bus_code_name,bus_station_name,officelineid,line,direction,alternative,line_type,start_stopcode,end_stopcode,week_period,bus_direction\n"
            "Beer Sheva,Dan,11,Station D,3,4,1,,City,10,20,Weekday,East\n"
        )

        with self.assertRaises(CommandError):
            call_command("import_bus_info_per_train_station", "--file", str(csv_path))

    def test_hebrew_excel_headers_are_mapped(self):
        xlsx_path = self._write_xlsx(
            [
                {
                    "שם תחנת הרכבת": "אשקלון",
                    "מפעיל": "אגד",
                    "קוד תחנת אוטובוס": 123,
                    "שם תחנת אוטובוס": "תחנה",
                    'מק"ט': 7,
                    "שילוט": 9,
                    "כיוון": 1,
                    "חלופה": "",
                    "סוג קו": "עירוני",
                    "מוצא": "100",
                    "יעד": "200",
                    "תקופת שבוע": "Weekday",
                    "כיוון נסיעת האוטובוס": "North",
                }
            ]
        )

        call_command("import_bus_info_per_train_station", "--file", str(xlsx_path))

        self.assertTrue(ConvergenceTable.objects.filter(train_station_name="אשקלון", operator="אגד").exists())
