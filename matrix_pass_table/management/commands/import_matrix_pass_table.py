import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from matrix_pass_table.models import PassengerMatrix


REQUIRED_COLUMNS = (
    "from_station_name",
    "to_station_name",
    "month",
    "year",
    "sum_values_pass",
)


class Command(BaseCommand):
    help = "Import rows into matrix_pass_table_passengermatrix from a CSV file using upsert semantics."

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            required=True,
            help=(
                "Path to CSV file with exact columns: "
                "from_station_name,to_station_name,month,year,sum_values_pass"
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and report insert/update counts without writing to DB.",
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
        csv_path = Path(options["file"]).expanduser()
        dry_run = options["dry_run"]
        strict = options["strict"]
        batch_size = options["batch_size"]

        if batch_size <= 0:
            raise CommandError("--batch-size must be a positive integer.")

        if not csv_path.exists():
            raise CommandError(f"CSV file not found: {csv_path}")

        rows = self._read_rows(csv_path)
        if not rows:
            self.stdout.write(self.style.WARNING("No rows found. Nothing to import."))
            return

        self._validate_header(rows[0].keys())

        if strict and not dry_run:
            with transaction.atomic():
                totals = self._process_rows(rows, dry_run=dry_run, strict=strict, batch_size=batch_size)
        else:
            totals = self._process_rows(rows, dry_run=dry_run, strict=strict, batch_size=batch_size)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Import completed."))
        self.stdout.write(f"Rows processed: {totals['total_rows']}")
        self.stdout.write(f"Inserted: {totals['inserted']}")
        self.stdout.write(f"Updated: {totals['updated']}")
        self.stdout.write(f"Invalid: {totals['invalid']}")
        self.stdout.write(f"Dry run: {'yes' if dry_run else 'no'}")

        if strict and totals["invalid"] > 0:
            raise CommandError("Import failed in --strict mode due to invalid rows.")

    def _process_rows(self, rows, dry_run, strict, batch_size):
        totals = {"total_rows": 0, "inserted": 0, "updated": 0, "invalid": 0}

        for index, row in enumerate(rows, start=2):
            totals["total_rows"] += 1
            try:
                payload = self._normalize_row(row, index)
                created = self._upsert(payload, dry_run=dry_run)
                if created:
                    totals["inserted"] += 1
                else:
                    totals["updated"] += 1
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

    def _read_rows(self, csv_path: Path):
        with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.DictReader(fp)
            if reader.fieldnames is None:
                raise CommandError("CSV file is missing a header row.")
            return list(reader)

    def _validate_header(self, columns):
        missing = [name for name in REQUIRED_COLUMNS if name not in columns]
        extras = [name for name in columns if name not in REQUIRED_COLUMNS]
        if missing or extras:
            details = []
            if missing:
                details.append(f"missing columns: {', '.join(missing)}")
            if extras:
                details.append(f"unexpected columns: {', '.join(extras)}")
            raise CommandError(
                "CSV columns must match exactly the required schema. " + "; ".join(details)
            )

    def _normalize_row(self, row, row_number):
        from_station_name = str(row["from_station_name"]).strip()
        to_station_name = str(row["to_station_name"]).strip()

        try:
            month = int(str(row["month"]).strip())
            year = int(str(row["year"]).strip())
            sum_values_pass = int(str(row["sum_values_pass"]).strip())
        except (TypeError, ValueError) as exc:
            raise CommandError(f"Row {row_number}: numeric fields must be valid integers.") from exc

        if not from_station_name:
            raise CommandError(f"Row {row_number}: from_station_name cannot be blank.")
        if not to_station_name:
            raise CommandError(f"Row {row_number}: to_station_name cannot be blank.")
        if not 1 <= month <= 12:
            raise CommandError(f"Row {row_number}: month must be between 1 and 12.")

        return {
            "from_station_name": from_station_name,
            "to_station_name": to_station_name,
            "month": month,
            "year": year,
            "sum_values_pass": sum_values_pass,
        }

    def _upsert(self, payload, dry_run=False):
        lookup = {
            "from_station_name": payload["from_station_name"],
            "to_station_name": payload["to_station_name"],
            "month": payload["month"],
            "year": payload["year"],
        }
        defaults = {"sum_values_pass": payload["sum_values_pass"]}

        if dry_run:
            exists = PassengerMatrix.objects.filter(**lookup).exists()
            return not exists

        _, created = PassengerMatrix.objects.update_or_create(defaults=defaults, **lookup)
        return created
