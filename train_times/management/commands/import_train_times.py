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
    "PassengersAscending",
    "PassengersDescending",
)

ARRIVAL_TIME_COLUMN = "Planned_Train_Arrivel_Time"
DEPARTURE_TIME_COLUMN = "Planned_Train_Departure_Time"


class Command(BaseCommand):
    help = (
        "Import rows into train_times_traintime from arrival/departure CSV files "
        "using deduplicating insert semantics."
    )

    def add_arguments(self, parser):
        parser.add_argument("--arrival-file", help="Path to arrival CSV file.")
        parser.add_argument("--departure-file", help="Path to departure CSV file.")
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
        arrival_file = options.get("arrival_file")
        departure_file = options.get("departure_file")
        dry_run = options["dry_run"]
        strict = options["strict"]
        batch_size = options["batch_size"]

        if not arrival_file and not departure_file:
            raise CommandError("At least one of --arrival-file or --departure-file is required.")
        if batch_size <= 0:
            raise CommandError("--batch-size must be a positive integer.")

        payloads = []
        if arrival_file:
            payloads.extend(self._load_file(arrival_file, event_type=TrainTime.EventType.ARRIVAL, strict=strict))
        if departure_file:
            payloads.extend(self._load_file(departure_file, event_type=TrainTime.EventType.DEPARTURE, strict=strict))

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

    def _load_file(self, file_path, event_type, strict):
        source_path = Path(file_path).expanduser()
        if not source_path.exists():
            raise CommandError(f"Source file not found: {source_path}")

        with source_path.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.DictReader(fp)
            if reader.fieldnames is None:
                raise CommandError(f"{source_path}: missing header row.")
            rows = list(reader)

        missing = [col for col in REQUIRED_BASE_COLUMNS if col not in reader.fieldnames]
        time_column = ARRIVAL_TIME_COLUMN if event_type == TrainTime.EventType.ARRIVAL else DEPARTURE_TIME_COLUMN
        if time_column not in reader.fieldnames:
            missing.append(time_column)
        if missing:
            raise CommandError(f"{source_path}: missing required columns: {', '.join(missing)}")

        payloads = []
        for index, row in enumerate(rows, start=2):
            try:
                payloads.append(self._normalize_row(row, index, event_type))
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

    def _normalize_row(self, row, row_number, event_type):
        station_name = "" if row.get("StationName") is None else str(row["StationName"]).strip()
        week_period = "" if row.get("WeekPeriod") is None else str(row["WeekPeriod"]).strip()

        if not week_period:
            raise CommandError(f"Row {row_number}: WeekPeriod cannot be blank.")

        time_col = ARRIVAL_TIME_COLUMN if event_type == TrainTime.EventType.ARRIVAL else DEPARTURE_TIME_COLUMN
        planned_time = self._normalize_time(row.get(time_col), row_number)

        return {
            "Year": self._normalize_int(row.get("Year"), "Year", row_number),
            "Month": self._normalize_int(row.get("Month"), "Month", row_number),
            "WeekPeriod": week_period,
            "train_station_code": self._normalize_int(row.get("train_station_code"), "train_station_code", row_number),
            "StationName": station_name,
            "Train_number": self._normalize_int(row.get("Train_number"), "Train_number", row_number),
            "event_type": event_type,
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
