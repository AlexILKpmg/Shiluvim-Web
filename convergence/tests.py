import json

from django.test import Client, TestCase

from convergence.management.commands.import_convergence import (
    BUS_TO_RAIL_OPTIONAL,
    RAIL_TO_BUS_OPTIONAL,
    Command,
)
from convergence.models import ConvergenceBusToRail, ConvergenceRailToBus


class ConvergenceViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    def test_view_uses_directional_rishui_fields_without_attribute_errors(self):
        ConvergenceBusToRail.objects.create(
            year=2026,
            month=1,
            week_period="יום חול",
            train_station_name="תל אביב סבידור מרכז",
            rail_direction="לכיוון תל אביב",
            train_number=101,
            operator="דן",
            rishui_train_departure_time="08:10",
        )
        ConvergenceRailToBus.objects.create(
            year=2026,
            month=1,
            week_period="יום חול",
            train_station_name="תל אביב סבידור מרכז",
            rail_direction="מכיוון תל אביב",
            train_number=202,
            operator="אגד",
            rishui_train_arrival_time="09:20",
        )

        response = self.client.get(
            "/convergence/",
            {"station": "תל אביב סבידור מרכז", "year": "2026", "month": "1"},
        )

        self.assertEqual(response.status_code, 200)
        bus_rows = json.loads(response.context["bus_to_rail_df_js"])
        rail_rows = json.loads(response.context["rail_to_bus_df_js"])

        self.assertEqual(bus_rows[0]["זמן יציאת הרכבת מהתחנה (רישוי)"], "08:10")
        self.assertEqual(bus_rows[0]["זמן הגעת הרכבת לתחנה (רישוי)"], "")

        self.assertEqual(rail_rows[0]["זמן יציאת הרכבת מהתחנה (רישוי)"], "")
        self.assertEqual(rail_rows[0]["זמן הגעת הרכבת לתחנה (רישוי)"], "09:20")


class ConvergenceImportNormalizeTests(TestCase):
    def test_normalize_row_maps_new_common_and_rishui_fields(self):
        cmd = Command()

        bus_row = {
            "שנה": 2026,
            "חודש": 1,
            "שם תחנת הרכבת": "תל אביב סבידור מרכז",
            "כיוון נסיעת הרכבת": "לכיוון תל אביב",
            "מספר הרכבת": 101,
            "מפעיל": "דן",
            'מק"ט': 12345,
            "כיוון": 1,
            "שילוט": 66,
            "רכבת זהב": "כן",
            "האם האוטובוס מגיע בזמן": 1,
            "זמן יציאת הרכבת מהתחנה (רישוי)": "08:10",
            "זמן הגעה לתחנה": "08:04",
        }
        bus_payload = cmd._normalize_row(
            bus_row,
            "יום חול",
            BUS_TO_RAIL_OPTIONAL,
            "WeekDay_rail_bus_convergence_2026-01.xlsx",
            "bus_to_rail",
            2,
        )
        self.assertEqual(bus_payload["train_number"], 101)
        self.assertEqual(bus_payload["makat"], 12345)
        self.assertEqual(bus_payload["direction"], 1)
        self.assertEqual(bus_payload["signage"], 66)
        self.assertEqual(bus_payload["is_gold_train"], "כן")
        self.assertEqual(bus_payload["is_bus_on_time"], 1)
        self.assertEqual(bus_payload["rishui_train_departure_time"], "08:10")

        rail_row = {
            "שנה": 2026,
            "חודש": 1,
            "שם תחנת הרכבת": "תל אביב סבידור מרכז",
            "כיוון נסיעת הרכבת": "מכיוון תל אביב",
            "מספר הרכבת": 202,
            "מפעיל": "אגד",
            'מק"ט': 67890,
            "כיוון": 2,
            "שילוט": 16,
            "רכבת זהב": "לא",
            "האם האוטובוס מגיע בזמן": 0,
            "זמן הגעת הרכבת לתחנה (רישוי)": "09:20",
            "זמן יציאה מהתחנה (רישוי)": "09:28",
        }
        rail_payload = cmd._normalize_row(
            rail_row,
            "יום חול",
            RAIL_TO_BUS_OPTIONAL,
            "WeekDay_rail_bus_convergence_2026-01.xlsx",
            "rail_to_bus",
            2,
        )
        self.assertEqual(rail_payload["train_number"], 202)
        self.assertEqual(rail_payload["makat"], 67890)
        self.assertEqual(rail_payload["direction"], 2)
        self.assertEqual(rail_payload["signage"], 16)
        self.assertEqual(rail_payload["is_gold_train"], "לא")
        self.assertEqual(rail_payload["is_bus_on_time"], 0)
        self.assertEqual(rail_payload["rishui_train_arrival_time"], "09:20")
