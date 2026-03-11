import csv
from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from bus_info_per_train_station_table.models import BusInfo


REQUIRED_COLUMNS = (
    "train_station_name",
    "operator",
    "bus_code_name",
    "bus_station_name",
    "officelineid",
    "line",
    "direction",
    "alternative",
    "line_type",
    "start_stopcode",
    "end_stopcode",
    "week_period",
    "bus_direction",
)

COLUMN_ALIASES = {
    # Hebrew headers from tables/for_convergence_data.xlsx
    "שם תחנת הרכבת": "train_station_name",
    "מפעיל": "operator",
    "קוד תחנת אוטובוס": "bus_code_name",
    "שם תחנת אוטובוס": "bus_station_name",
    'מק"ט': "officelineid",
    "שילוט": "line",
    "כיוון": "direction",
    "חלופה": "alternative",
    "סוג קו": "line_type",
    "מוצא": "start_stopcode",
    "יעד": "end_stopcode",
    "תקופת שבוע": "week_period",
    "כיוון נסיעת האוטובוס": "bus_direction",
    # Common English variants
    "station_name": "train_station_name",
    "OfficeLineID": "officelineid",
    "LineType": "line_type",
    "Start_StopCode": "start_stopcode",
    "End_StopCode": "end_stopcode",
    "WeekPeriod": "week_period",
    "Bus_Direction": "bus_direction",
}

INTEGER_FIELDS = ("bus_code_name", "officelineid", "line", "direction")
REQUIRED_TEXT_FIELDS = ("train_station_name", "operator", "bus_station_name")
OPTIONAL_TEXT_FIELDS = ("alternative", "line_type", "start_stopcode", "end_stopcode", "week_period", "bus_direction")


class Command(BaseCommand):
    help = (
        "Import rows into bus_info_per_train_station_table_businfo "
        "from CSV/XLSX using deduplicating insert semantics."
    )

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Path to source file (.csv or .xlsx).")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and report insert/existing counts without writing to DB.",
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
            help="Reserved for future bulk operations; currently used for progress reporting only.",
        )

    def handle(self, *args, **options):
        source_path = Path(options["file"]).expanduser()
        dry_run = options["dry_run"]
        strict = options["strict"]
        batch_size = options["batch_size"]

        if batch_size <= 0:
            raise CommandError("--batch-size must be a positive integer.")
        if not source_path.exists():
            raise CommandError(f"Source file not found: {source_path}")

        rows = self._read_rows(source_path)
        if not rows:
            self.stdout.write(self.style.WARNING("No rows found. Nothing to import."))
            return

        normalized_rows = []
        for index, row in enumerate(rows, start=2):
            try:
                normalized_rows.append(self._normalize_row(row, index))
            except CommandError:
                if strict:
                    raise
                normalized_rows.append({"__invalid__": True, "__row_number__": index, "__raw__": row})

        if strict and not dry_run:
            with transaction.atomic():
                totals = self._process_rows(normalized_rows, dry_run=dry_run, strict=strict, batch_size=batch_size)
        else:
            totals = self._process_rows(normalized_rows, dry_run=dry_run, strict=strict, batch_size=batch_size)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Import completed."))
        self.stdout.write(f"Rows processed: {totals['total_rows']}")
        self.stdout.write(f"Inserted: {totals['inserted']}")
        self.stdout.write(f"Existing (duplicate): {totals['existing']}")
        self.stdout.write(f"Invalid: {totals['invalid']}")
        self.stdout.write(f"Dry run: {'yes' if dry_run else 'no'}")

        if strict and totals["invalid"] > 0:
            raise CommandError("Import failed in --strict mode due to invalid rows.")

    def _read_rows(self, source_path: Path):
        suffix = source_path.suffix.lower()
        if suffix == ".csv":
            with source_path.open("r", encoding="utf-8-sig", newline="") as fp:
                reader = csv.DictReader(fp)
                if reader.fieldnames is None:
                    raise CommandError("CSV file is missing a header row.")
                return list(reader)

        if suffix in (".xlsx", ".xls"):
            df = pd.read_excel(source_path)
            return df.to_dict(orient="records")

        raise CommandError("Unsupported file extension. Use .csv, .xlsx, or .xls.")

    def _normalize_keys(self, row):
        normalized = {}
        for key, value in row.items():
            raw_key = str(key).strip()
            canonical_key = COLUMN_ALIASES.get(raw_key, raw_key)
            normalized[canonical_key] = value
        return normalized

    def _normalize_int(self, value, field_name, row_number):
        try:
            if value is None:
                raise ValueError
            text = str(value).strip()
            if text == "":
                raise ValueError
            return int(float(text))
        except (TypeError, ValueError) as exc:
            raise CommandError(f"Row {row_number}: {field_name} must be a valid integer.") from exc

    def _normalize_text(self, value):
        if value is None:
            return ""
        text = str(value).strip()
        if text.lower() == "nan":
            return ""
        return text

    def _normalize_row(self, row, row_number):
        row = self._normalize_keys(row)

        missing = [name for name in REQUIRED_COLUMNS if name not in row]
        extras = [name for name in row if name not in REQUIRED_COLUMNS]
        if missing or extras:
            details = []
            if missing:
                details.append(f"missing columns: {', '.join(missing)}")
            if extras:
                details.append(f"unexpected columns: {', '.join(extras)}")
            raise CommandError(
                f"Row {row_number}: source columns must match expected schema after alias mapping; "
                + "; ".join(details)
            )

        normalized = {}
        for field_name in INTEGER_FIELDS:
            normalized[field_name] = self._normalize_int(row[field_name], field_name, row_number)

        for field_name in REQUIRED_TEXT_FIELDS:
            normalized[field_name] = self._normalize_text(row[field_name])
            if not normalized[field_name]:
                raise CommandError(f"Row {row_number}: {field_name} cannot be blank.")

        for field_name in OPTIONAL_TEXT_FIELDS:
            normalized[field_name] = self._normalize_text(row[field_name])

        return normalized

    def _process_rows(self, rows, dry_run, strict, batch_size):
        totals = {"total_rows": 0, "inserted": 0, "existing": 0, "invalid": 0}

        for row in rows:
            totals["total_rows"] += 1

            if row.get("__invalid__"):
                totals["invalid"] += 1
                if strict:
                    raise CommandError(f"Row {row['__row_number__']}: invalid data.")
            else:
                try:
                    created = self._insert_if_new(row, dry_run=dry_run)
                    if created:
                        totals["inserted"] += 1
                    else:
                        totals["existing"] += 1
                except CommandError as exc:
                    totals["invalid"] += 1
                    if strict:
                        raise
                    self.stderr.write(self.style.WARNING(str(exc)))

            if totals["total_rows"] % batch_size == 0:
                self.stdout.write(
                    f"Processed {totals['total_rows']} rows "
                    f"(inserted={totals['inserted']}, existing={totals['existing']}, invalid={totals['invalid']})"
                )

        return totals

    def _insert_if_new(self, payload, dry_run=False):
        if dry_run:
            exists = BusInfo.objects.filter(**payload).exists()
            return not exists

        _, created = BusInfo.objects.get_or_create(**payload)
        return created
