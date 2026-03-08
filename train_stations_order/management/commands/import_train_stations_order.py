import csv
from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from train_stations_order.models import Ranking


CANONICAL_COLUMNS = ("train_num", "train_station_id", "train_station_order", "train_station_name")
COLUMN_ALIASES = {
    "Train_number": "train_num",
    "train_station_code": "train_station_id",
    "train_rishui_station_order_source": "train_station_order",
    "StationName": "train_station_name",
}


class Command(BaseCommand):
    help = (
        "Import rows into train_stations_order_ranking from CSV/XLSX "
        "using deduplicating insert semantics."
    )

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Path to source file (.csv/.xlsx/.xls).")
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

        payloads = []
        for index, row in enumerate(rows, start=2):
            try:
                payloads.append(self._normalize_row(row, index))
            except CommandError:
                if strict:
                    raise
                payloads.append({"__invalid__": True, "__row_number__": index})

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

    def _read_rows(self, source_path: Path):
        suffix = source_path.suffix.lower()
        if suffix == ".csv":
            with source_path.open("r", encoding="utf-8-sig", newline="") as fp:
                reader = csv.DictReader(fp)
                if reader.fieldnames is None:
                    raise CommandError("CSV file is missing a header row.")
                return list(reader)

        if suffix in (".xlsx", ".xls"):
            return pd.read_excel(source_path).to_dict(orient="records")

        raise CommandError("Unsupported file extension. Use .csv, .xlsx, or .xls.")

    def _normalize_keys(self, row):
        normalized = {}
        for key, value in row.items():
            raw_key = str(key).strip()
            canonical = COLUMN_ALIASES.get(raw_key, raw_key)
            normalized[canonical] = value
        return normalized

    def _normalize_int(self, value, field_name, row_number):
        text = "" if value is None else str(value).strip()
        if text == "" or text.lower() == "nan":
            raise CommandError(f"Row {row_number}: {field_name} must be a valid integer.")
        try:
            return int(float(text))
        except (TypeError, ValueError) as exc:
            raise CommandError(f"Row {row_number}: {field_name} must be a valid integer.") from exc

    def _normalize_row(self, row, row_number):
        row = self._normalize_keys(row)
        missing = [name for name in CANONICAL_COLUMNS if name not in row]
        if missing:
            raise CommandError(f"Row {row_number}: missing columns: {', '.join(missing)}")

        train_station_name = "" if row.get("train_station_name") is None else str(row["train_station_name"]).strip()
        if not train_station_name:
            raise CommandError(f"Row {row_number}: train_station_name cannot be blank.")

        return {
            "train_num": self._normalize_int(row.get("train_num"), "train_num", row_number),
            "train_station_id": self._normalize_int(row.get("train_station_id"), "train_station_id", row_number),
            "train_station_order": self._normalize_int(row.get("train_station_order"), "train_station_order", row_number),
            "train_station_name": train_station_name,
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
                created = self._insert_if_new(payload, dry_run=dry_run)
                if created:
                    totals["inserted"] += 1
                else:
                    totals["existing"] += 1

            if totals["total_rows"] % batch_size == 0:
                self.stdout.write(
                    f"Processed {totals['total_rows']} rows "
                    f"(inserted={totals['inserted']}, existing={totals['existing']}, invalid={totals['invalid']})"
                )

        return totals

    def _insert_if_new(self, payload, dry_run=False):
        if dry_run:
            exists = Ranking.objects.filter(**payload).exists()
            return not exists

        _, created = Ranking.objects.get_or_create(**payload)
        return created
