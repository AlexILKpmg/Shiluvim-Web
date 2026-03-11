import csv
from datetime import time
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from train_times.models import TrainTime


REQUIRED_BASE_COLUMNS = (
    "Year",
    "Month",
    "WeekPeriod",
    "train_station_code",
    "StationName",
    "Train_number",
    "Planned_Train_Arrivel_Time",
    "PassengersAscending",
    "PassengersDescending",
    "event_type",
)


class Command(BaseCommand):
    help = (
        "Import rows into train_times_traintime from a unified CSV file "
        "using deduplicating insert semantics."
    )

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Path to train-times CSV file.")
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
        source_file = options.get("file")
        dry_run = options["dry_run"]
        strict = options["strict"]
        batch_size = options["batch_size"]

        if batch_size <= 0:
            raise CommandError("--batch-size must be a positive integer.")

        payloads = self._load_file(source_file, strict=strict)

        if strict and not dry_run:
            with transaction.atomic():
                totals = self._process_rows(payloads, dry_run=dry_run, strict=strict, batch_size=batch_size)
        else:
            totals = self._process_rows(payloads, dry_run=dry_run, strict=strict, batch_size=batch_size)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Import completed."))
        self.stdout.write(f"Rows processed: {totals['total_rows']}")
        self.stdout.write(f"Inserted: {totals['inserted']}")
        self.stdout.write(f"Existing (duplicate): {totals['existing']}")
        self.stdout.write(f"Invalid: {totals['invalid']}")
        self.stdout.write(f"Dry run: {'yes' if dry_run else 'no'}")

        if strict and totals["invalid"] > 0:
            raise CommandError("Import failed in --strict mode due to invalid rows.")

    def _load_file(self, file_path, strict):
        source_path = Path(file_path).expanduser()
        if not source_path.exists():
            raise CommandError(f"Source file not found: {source_path}")

        with source_path.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.DictReader(fp)
            if reader.fieldnames is None:
                raise CommandError(f"{source_path}: missing header row.")
            rows = list(reader)

        missing = [col for col in REQUIRED_BASE_COLUMNS if col not in reader.fieldnames]
        if missing:
            raise CommandError(f"{source_path}: missing required columns: {', '.join(missing)}")

        payloads = []
        for index, row in enumerate(rows, start=2):
            try:
                payloads.append(self._normalize_row(row, index))
            except CommandError:
                if strict:
                    raise
                payloads.append({"__invalid__": True, "__row_number__": index})
        return payloads

    def _normalize_int(self, value, field_name, row_number):
        text = "" if value is None else str(value).strip()
        if text == "":
            raise CommandError(f"Row {row_number}: {field_name} must be a valid integer.")
        try:
            return int(float(text))
        except (TypeError, ValueError) as exc:
            raise CommandError(f"Row {row_number}: {field_name} must be a valid integer.") from exc

    def _normalize_passenger(self, value):
        text = "" if value is None else str(value).strip()
        if text == "" or text.lower() == "nan":
            return 0
        return int(float(text))

    def _normalize_time(self, value, row_number):
        text = "" if value is None else str(value).strip()
        if not text:
            raise CommandError(f"Row {row_number}: planned_time is required.")
        try:
            parts = text.split(":")
            if len(parts) != 3:
                raise ValueError
            h, m, s = [int(p) for p in parts]
            return time(hour=h, minute=m, second=s)
        except (TypeError, ValueError) as exc:
            raise CommandError(f"Row {row_number}: planned_time must be HH:MM:SS.") from exc

    def _normalize_event_type(self, value, row_number):
        event_type = "" if value is None else str(value).strip().lower()
        if event_type == TrainTime.EventType.TO_TLV:
            return TrainTime.EventType.TO_TLV
        if event_type == TrainTime.EventType.FROM_TLV:
            return TrainTime.EventType.FROM_TLV
        raise CommandError(f"Row {row_number}: event_type must be 'to_tlv' or 'from_tlv'.")

    def _normalize_row(self, row, row_number):
        station_name = "" if row.get("StationName") is None else str(row["StationName"]).strip()
        week_period = "" if row.get("WeekPeriod") is None else str(row["WeekPeriod"]).strip()

        if not week_period:
            raise CommandError(f"Row {row_number}: WeekPeriod cannot be blank.")

        planned_time = self._normalize_time(row.get("Planned_Train_Arrivel_Time"), row_number)

        return {
            "Year": self._normalize_int(row.get("Year"), "Year", row_number),
            "Month": self._normalize_int(row.get("Month"), "Month", row_number),
            "WeekPeriod": week_period,
            "train_station_code": self._normalize_int(row.get("train_station_code"), "train_station_code", row_number),
            "StationName": station_name,
            "Train_number": self._normalize_int(row.get("Train_number"), "Train_number", row_number),
            "event_type": self._normalize_event_type(row.get("event_type"), row_number),
            "planned_time": planned_time,
            "PassengersAscending": self._normalize_passenger(row.get("PassengersAscending")),
            "PassengersDescending": self._normalize_passenger(row.get("PassengersDescending")),
        }

    def _process_rows(self, payloads, dry_run, strict, batch_size):
        totals = {"total_rows": 0, "inserted": 0, "existing": 0, "invalid": 0}

        for payload in payloads:
            totals["total_rows"] += 1
            if payload.get("__invalid__"):
                totals["invalid"] += 1
                if strict:
                    raise CommandError(f"Row {payload['__row_number__']}: invalid data.")
            else:
                try:
                    created = self._insert_if_new(payload, dry_run=dry_run)
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
            exists = TrainTime.objects.filter(**payload).exists()
            return not exists
        _, created = TrainTime.objects.get_or_create(**payload)
        return created
