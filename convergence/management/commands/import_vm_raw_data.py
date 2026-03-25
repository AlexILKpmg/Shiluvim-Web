import re
from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.utils import DatabaseError, ProgrammingError

from convergence.models import VM_raw_data


FILE_RE = re.compile(r"^(WeekDay|Friday|Saturday)_vm_data_(\d{4})-(\d{2})\.csv$", re.IGNORECASE)
WEEK_LABELS = {
    "WeekDay": "יום חול",
    "Friday": "שישי",
    "Saturday": "שבת",
}

CSV_TO_MODEL = {
    "OfficeLineID": "makat",
    "Direction": "direction",
    "Alternative": "alternative",
    "TripStartTime": "departure_time",
    "ArrivalTime": "arrival_time_to_station_raw",
}


class Command(BaseCommand):
    help = (
        "Import VM raw data from CSV files into convergence_vm_raw_data. "
        "Year/month/week_period are derived from filename."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            action="append",
            default=[],
            help="Path to a vm_data csv file. Can be passed multiple times.",
        )
        parser.add_argument(
            "--dir",
            default="tables",
            help="Directory to scan for *_vm_data_YYYY-MM.csv files.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and report counts without writing to DB.",
        )

    def handle(self, *args, **options):
        files = self._resolve_files(options["file"], options["dir"])
        if not files:
            raise CommandError("No source files found. Use --file or provide a --dir with matching files.")

        dry_run = options["dry_run"]
        self._ensure_table_available()

        totals = {"files": 0, "rows_processed": 0, "rows_written": 0}
        for source_path in files:
            week_period, year, month = self._parse_file_name(source_path.name)
            rows = self._load_rows(source_path, year, month, week_period)
            totals["files"] += 1
            totals["rows_processed"] += len(rows)

            if dry_run:
                self.stdout.write(f"[dry-run] {source_path.name}: would write {len(rows)} rows")
                continue

            with transaction.atomic():
                VM_raw_data.objects.filter(year=year, month=month, week_period=week_period).delete()
                VM_raw_data.objects.bulk_create(rows, batch_size=2000)

            totals["rows_written"] += len(rows)
            self.stdout.write(f"{source_path.name}: wrote {len(rows)} rows")

        self.stdout.write(self.style.SUCCESS("Import completed."))
        self.stdout.write(f"Files processed: {totals['files']}")
        self.stdout.write(f"Rows processed: {totals['rows_processed']}")
        self.stdout.write(f"Rows written: {totals['rows_written']}")
        self.stdout.write(f"Dry run: {'yes' if dry_run else 'no'}")

    def _resolve_files(self, file_args, scan_dir):
        files = []

        for raw in file_args:
            p = Path(raw).expanduser()
            if not p.exists() or p.suffix.lower() != ".csv":
                raise CommandError(f"Invalid --file path (must exist and be .csv): {p}")
            files.append(p)

        if not file_args:
            root = Path(scan_dir).expanduser()
            if not root.exists():
                raise CommandError(f"Directory not found: {root}")
            files.extend(sorted(root.glob("*_vm_data_*.csv")))

        valid = []
        for p in sorted(set(files)):
            if not FILE_RE.match(p.name):
                raise CommandError(
                    f"Unexpected vm data filename format: {p.name}. "
                    "Expected: WeekDay|Friday|Saturday_vm_data_YYYY-MM.csv"
                )
            valid.append(p)

        return valid

    def _parse_file_name(self, file_name):
        match = FILE_RE.match(file_name)
        if not match:
            raise CommandError(
                f"Unexpected vm data filename format: {file_name}. "
                "Expected: WeekDay|Friday|Saturday_vm_data_YYYY-MM.csv"
            )
        week_key = match.group(1)
        year = int(match.group(2))
        month = int(match.group(3))
        week_key = week_key[0].upper() + week_key[1:]
        week_period = WEEK_LABELS.get(week_key, week_key)
        return week_period, year, month

    def _load_rows(self, source_path, year, month, week_period):
        try:
            df = pd.read_csv(source_path)
        except Exception as exc:
            raise CommandError(f"{source_path.name}: failed to read csv: {exc}") from exc

        missing = [col for col in CSV_TO_MODEL if col not in df.columns]
        if missing:
            raise CommandError(f"{source_path.name}: missing required columns: {', '.join(missing)}")

        rows = []
        for record in df.to_dict(orient="records"):
            rows.append(
                VM_raw_data(
                    year=year,
                    month=month,
                    week_period=week_period,
                    makat=self._to_int_or_none(record.get("OfficeLineID")),
                    direction=self._to_int_or_none(record.get("Direction")),
                    alternative=self._clean_text(record.get("Alternative")),
                    departure_time=self._clean_text(record.get("TripStartTime")),
                    arrival_time_to_station_raw=self._clean_text(record.get("ArrivalTime")),
                )
            )
        return rows

    def _ensure_table_available(self):
        try:
            VM_raw_data.objects.exists()
        except (ProgrammingError, DatabaseError) as exc:
            raise CommandError("VM table is not ready. Run: python manage.py migrate convergence") from exc

    def _clean_text(self, value):
        if value is None:
            return ""
        text = str(value).strip()
        if text.lower() == "nan":
            return ""
        return text

    def _to_int_or_none(self, value):
        text = self._clean_text(value)
        if not text:
            return None
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None
