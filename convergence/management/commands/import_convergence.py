import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.utils import DatabaseError, ProgrammingError

from convergence.models import ConvergenceBusToRail, ConvergenceRailToBus


SHEET_BUS_TO_RAIL = "bus_to_rail"
SHEET_RAIL_TO_BUS = "rail_to_bus"

FILE_RE = re.compile(r"^(WeekDay|Friday|Saturday)_rail_bus_convergence_(\d{4}-\d{2})\.xlsx$", re.IGNORECASE)
WEEK_LABELS = {
    "WeekDay": "יום חול",
    "Friday": "שישי",
    "Saturday": "שבת",
}

COMMON_REQUIRED = {
    "שנה": "year",
    "חודש": "month",
    "שם תחנת הרכבת": "train_station_name",
    "כיוון נסיעת הרכבת": "rail_direction",
    "מספר הרכבת": "train_number",
    "מפעיל": "operator",
}

COMMON_OPTIONAL = {
    'מק"ט': "makat",
    "כיוון": "direction",
    "חלופה": "alternative",
    "זמן יציאה": "departure_time",
    "ממוצע נוסעים לנסיעה": "avg_passengers_per_trip",
    "שילוט": "signage",
    "רכבת זהב": "is_gold_train",
    "האם האוטובוס מגיע בזמן": "is_bus_on_time",
}

BUS_TO_RAIL_OPTIONAL = {
    "זמן הגעת הרכבת לתחנה (רישוי)": "rishui_train_arrival_time",
    "זמן הגעה לתחנה": "arrival_time_to_station",
    "טווח זמן ההגעה לתחנה": "arrival_time_window",
    "הפרש בדקות (מאוטובוס לרכבת)": "minutes_gap_bus_to_rail",
    "המלצה (דקות)": "recommended_minutes",
    "מספר תצפיות": "observations_count",
    "מספר הנסיעות שעמדו בזמנים": "on_time_count",
    "אחוז הנסיעות שעמדו בזמנים": "on_time_percentage",
}

RAIL_TO_BUS_OPTIONAL = {
    "זמן הגעת הרכבת לתחנה (רישוי)": "rishui_train_arrival_time",
    "הפרש בדקות (מרכבת לאוטובוס)": "minutes_gap_rail_to_bus",
    "המלצה (דקות)": "recommended_minutes",
}

BUS_LOOKUP_FIELDS = (
    "year",
    "month",
    "week_period",
    "train_station_name",
    "rail_direction",
    "train_number",
    "makat",
    "departure_time",
)

RAIL_LOOKUP_FIELDS = (
    "year",
    "month",
    "week_period",
    "train_station_name",
    "rail_direction",
    "train_number",
    "makat",
    "departure_time",
)


class Command(BaseCommand):
    help = (
        "Import convergence rows from XLSX files into convergence_convergencebustorail "
        "and convergence_convergencerailtobus using upsert semantics."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            action="append",
            default=[],
            help="Path to a convergence xlsx file. Can be passed multiple times.",
        )
        parser.add_argument(
            "--dir",
            default="tables",
            help="Directory to scan for *_rail_bus_convergence_YYYY-MM.xlsx files.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and report counts without writing to DB.",
        )
        parser.add_argument(
            "--strict",
            action="store_true",
            help="Fail immediately on first invalid row or DB error.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Progress reporting interval.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        strict = options["strict"]
        batch_size = options["batch_size"]

        if batch_size <= 0:
            raise CommandError("--batch-size must be a positive integer.")

        files = self._resolve_files(options["file"], options["dir"])
        if not files:
            raise CommandError("No source files found. Use --file or provide a --dir with matching files.")
        self._ensure_tables_available()

        if strict and not dry_run:
            with transaction.atomic():
                totals = self._process_files(files, dry_run=dry_run, strict=strict, batch_size=batch_size)
        else:
            totals = self._process_files(files, dry_run=dry_run, strict=strict, batch_size=batch_size)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Import completed."))
        self.stdout.write(f"Files processed: {totals['files']}")
        self.stdout.write(f"Rows processed: {totals['total_rows']}")
        self.stdout.write(f"Inserted: {totals['inserted']}")
        self.stdout.write(f"Updated: {totals['updated']}")
        self.stdout.write(f"Invalid: {totals['invalid']}")
        self.stdout.write(f"Dry run: {'yes' if dry_run else 'no'}")

        if strict and totals["invalid"] > 0:
            raise CommandError("Import failed in --strict mode due to invalid rows.")

    def _resolve_files(self, file_args, scan_dir):
        files = []

        for raw in file_args:
            p = Path(raw).expanduser()
            if not p.exists() or p.suffix.lower() != ".xlsx":
                raise CommandError(f"Invalid --file path (must exist and be .xlsx): {p}")
            files.append(p)

        if not file_args:
            root = Path(scan_dir).expanduser()
            if not root.exists():
                raise CommandError(f"Directory not found: {root}")
            files.extend(sorted(root.glob("*_rail_bus_convergence_*.xlsx")))

        return sorted(set(files))

    def _process_files(self, files, dry_run, strict, batch_size):
        totals = {"files": 0, "total_rows": 0, "inserted": 0, "updated": 0, "invalid": 0}

        for source_path in files:
            totals["files"] += 1
            week_period = self._derive_week_period(source_path.name)

            for sheet_name, model, sheet_optional, lookup_fields in (
                (SHEET_BUS_TO_RAIL, ConvergenceBusToRail, BUS_TO_RAIL_OPTIONAL, BUS_LOOKUP_FIELDS),
                (SHEET_RAIL_TO_BUS, ConvergenceRailToBus, RAIL_TO_BUS_OPTIONAL, RAIL_LOOKUP_FIELDS),
            ):
                try:
                    df = pd.read_excel(source_path, sheet_name=sheet_name)
                except Exception as exc:
                    msg = f"{source_path.name}/{sheet_name}: failed to read sheet: {exc}"
                    if strict:
                        raise CommandError(msg) from exc
                    self.stderr.write(self.style.WARNING(msg))
                    continue

                rows = df.to_dict(orient="records")
                for idx, row in enumerate(rows, start=2):
                    if self._is_empty_row(row):
                        continue
                    totals["total_rows"] += 1
                    try:
                        payload = self._normalize_row(row, week_period, sheet_optional, source_path.name, sheet_name, idx)
                        outcome = self._upsert(model, payload, lookup_fields, dry_run=dry_run)
                        totals[outcome] += 1
                    except CommandError as exc:
                        totals["invalid"] += 1
                        if strict:
                            raise
                        self.stderr.write(self.style.WARNING(str(exc)))

                    if totals["total_rows"] % batch_size == 0:
                        self.stdout.write(
                            f"Processed {totals['total_rows']} rows "
                            f"(inserted={totals['inserted']}, updated={totals['updated']}, invalid={totals['invalid']})"
                        )

        return totals

    def _ensure_tables_available(self):
        try:
            ConvergenceBusToRail.objects.exists()
            ConvergenceRailToBus.objects.exists()
        except (ProgrammingError, DatabaseError) as exc:
            raise CommandError(
                "Convergence tables are not ready. Run: python manage.py migrate convergence"
            ) from exc

    def _is_empty_row(self, row):
        for value in row.values():
            text = self._clean_text(value)
            if text != "":
                return False
        return True

    def _derive_week_period(self, file_name):
        match = FILE_RE.match(file_name)
        if not match:
            return ""
        week_key = match.group(1)
        week_key = week_key[0].upper() + week_key[1:]
        return WEEK_LABELS.get(week_key, "")

    def _normalize_row(self, row, week_period, sheet_optional, file_name, sheet_name, row_number):
        normalized = {}

        for src, dst in COMMON_REQUIRED.items():
            if src not in row:
                raise CommandError(f"{file_name}/{sheet_name} row {row_number}: missing required column '{src}'.")
            normalized[dst] = row[src]

        for src, dst in COMMON_OPTIONAL.items():
            normalized[dst] = row.get(src)

        for src, dst in sheet_optional.items():
            normalized[dst] = row.get(src)

        normalized["week_period"] = week_period or self._clean_text(row.get("תקופת שבוע"))

        payload = {
            "year": self._to_int(normalized["year"], "year", file_name, sheet_name, row_number),
            "month": self._to_int(normalized["month"], "month", file_name, sheet_name, row_number),
            "week_period": self._require_text(normalized["week_period"], "week_period", file_name, sheet_name, row_number),
            "train_station_name": self._require_text(normalized["train_station_name"], "train_station_name", file_name, sheet_name, row_number),
            "rail_direction": self._require_text(normalized["rail_direction"], "rail_direction", file_name, sheet_name, row_number),
            "train_number": self._to_int(normalized["train_number"], "train_number", file_name, sheet_name, row_number),
            "operator": self._require_text(normalized["operator"], "operator", file_name, sheet_name, row_number),
            "makat": self._to_int_or_none(normalized.get("makat")),
            "direction": self._to_int_or_none(normalized.get("direction")),
            "alternative": self._clean_text(normalized.get("alternative")),
            "departure_time": self._clean_text(normalized.get("departure_time")),
            "avg_passengers_per_trip": self._to_float_or_none(normalized.get("avg_passengers_per_trip")),
            "signage": self._to_int_or_none(normalized.get("signage")),
            "is_gold_train": self._clean_text(normalized.get("is_gold_train")),
            "is_bus_on_time": self._to_int_or_none(normalized.get("is_bus_on_time")),
        }

        if "arrival_time_to_station" in normalized:
            payload["rishui_train_arrival_time"] = self._clean_text(normalized.get("rishui_train_arrival_time"))
            payload["arrival_time_to_station"] = self._clean_text(normalized.get("arrival_time_to_station"))
            payload["arrival_time_window"] = self._clean_text(normalized.get("arrival_time_window"))
            payload["minutes_gap_bus_to_rail"] = self._to_float_or_none(normalized.get("minutes_gap_bus_to_rail"))
            payload["recommended_minutes"] = self._to_int_or_none(normalized.get("recommended_minutes"))
            payload["observations_count"] = self._to_int_or_none(normalized.get("observations_count"))
            payload["on_time_count"] = self._to_int_or_none(normalized.get("on_time_count"))
            payload["on_time_percentage"] = self._to_decimal_or_none(normalized.get("on_time_percentage"))

        if "minutes_gap_rail_to_bus" in normalized:
            payload["rishui_train_arrival_time"] = self._clean_text(normalized.get("rishui_train_arrival_time"))
            payload["minutes_gap_rail_to_bus"] = self._to_float_or_none(normalized.get("minutes_gap_rail_to_bus"))
            payload["recommended_minutes"] = self._to_int_or_none(normalized.get("recommended_minutes"))

        return payload

    def _upsert(self, model, payload, lookup_fields, dry_run=False):
        lookup = {k: payload[k] for k in lookup_fields}
        defaults = {k: v for k, v in payload.items() if k not in lookup_fields}
        if dry_run:
            obj = model.objects.filter(**lookup).first()
            if obj is None:
                return "inserted"
            return "updated"

        _, created = model.objects.update_or_create(**lookup, defaults=defaults)
        return "inserted" if created else "updated"

    def _clean_text(self, value):
        if value is None:
            return ""
        text = str(value).strip()
        if text.lower() == "nan":
            return ""
        return text

    def _require_text(self, value, field_name, file_name, sheet_name, row_number):
        text = self._clean_text(value)
        if not text:
            raise CommandError(f"{file_name}/{sheet_name} row {row_number}: {field_name} cannot be blank.")
        return text

    def _to_int(self, value, field_name, file_name, sheet_name, row_number):
        out = self._to_int_or_none(value)
        if out is None:
            raise CommandError(f"{file_name}/{sheet_name} row {row_number}: {field_name} must be a valid integer.")
        return out

    def _to_int_or_none(self, value):
        if value is None:
            return None
        text = self._clean_text(value)
        if not text:
            return None
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None

    def _to_float_or_none(self, value):
        if value is None:
            return None
        text = self._clean_text(value)
        if not text:
            return None
        text = text.replace(",", "")
        try:
            return float(text)
        except (TypeError, ValueError):
            return None

    def _to_decimal_or_none(self, value):
        if value is None:
            return None
        text = self._clean_text(value)
        if not text:
            return None
        text = text.replace("%", "").replace(",", "")
        try:
            return Decimal(text)
        except (InvalidOperation, ValueError):
            return None
