# Shiluvim-Web

## Import data into `rating_table_ranking`

1. Apply migrations for the `rating_table` app:

```bash
python manage.py migrate rating_table
```

2. Import CSV data with exact required headers:

```bash
python manage.py import_rating_table --file path/to/rating_table.csv
```

Required CSV columns (exact names and order recommended):

```text
year,month,train_station_name,ascending_pass,descending_pass,rank
```

Optional command flags:

```bash
python manage.py import_rating_table --file path/to/rating_table.csv --dry-run
python manage.py import_rating_table --file path/to/rating_table.csv --strict
python manage.py import_rating_table --file path/to/rating_table.csv --batch-size 500
```

Behavior:
- Upsert by `(year, month, train_station_name)`
- `month` must be between `1` and `12`
- In `--strict` mode, the command stops on first invalid row and rolls back all writes

## Import data into `matrix_pass_table_passengermatrix`

1. Apply migrations for the `matrix_pass_table` app:

```bash
python manage.py migrate matrix_pass_table
```

2. Import CSV data with exact required headers:

```bash
python manage.py import_matrix_pass_table --file path/to/matrix_pass_table.csv
```

Required CSV columns (exact names and order recommended):

```text
from_station_name,to_station_name,month,year,sum_values_pass
```

Optional command flags:

```bash
python manage.py import_matrix_pass_table --file path/to/matrix_pass_table.csv --dry-run
python manage.py import_matrix_pass_table --file path/to/matrix_pass_table.csv --strict
python manage.py import_matrix_pass_table --file path/to/matrix_pass_table.csv --batch-size 500
```

Behavior:
- Upsert by `(from_station_name, to_station_name, month, year)`
- `month` must be between `1` and `12`
- In `--strict` mode, the command stops on first invalid row and rolls back all writes

## Import data into `bus_info_per_train_station_table_convergencetable`

Import from `.csv` or `.xlsx`:

```bash
python manage.py import_bus_info_per_train_station --file path/to/for_convergence_data.xlsx
```

Required canonical columns:

```text
train_station_name,operator,bus_code_name,bus_station_name,officelineid,line,direction,alternative,line_type,start_stopcode,end_stopcode,week_period,bus_direction
```

The importer also supports the Hebrew Excel headers used in `tables/for_convergence_data.xlsx`.

Optional command flags:

```bash
python manage.py import_bus_info_per_train_station --file path/to/data.xlsx --dry-run
python manage.py import_bus_info_per_train_station --file path/to/data.xlsx --strict
python manage.py import_bus_info_per_train_station --file path/to/data.xlsx --batch-size 500
```

Behavior:
- Deduplicating insert: exact duplicate rows are skipped (existing)
- In `--strict` mode, the command stops on first invalid row and rolls back all writes

## Import data into `train_times_traintime`

1. Apply migrations for `train_times`:

```bash
python manage.py migrate train_times
```

2. Import arrival/departure CSV files:

```bash
python manage.py import_train_times --arrival-file tables/Arrivel_train_passengers_numbers.csv --departure-file tables/Departure_train_passengers_numbers.csv
```

Required arrival/departure columns:
- Shared: `Year,Month,WeekPeriod,train_station_code,StationName,Train_number,PassengersAscending,PassengersDescending`
- Arrival time: `Planned_Train_Arrivel_Time`
- Departure time: `Planned_Train_Departure_Time`

Optional command flags:

```bash
python manage.py import_train_times --arrival-file path/to/arrival.csv --dry-run
python manage.py import_train_times --departure-file path/to/departure.csv --strict
python manage.py import_train_times --arrival-file path/to/arrival.csv --departure-file path/to/departure.csv --batch-size 500
```

Behavior:
- Inserts rows with `event_type=ARRIVAL` for arrival file and `event_type=DEPARTURE` for departure file
- Blank passenger values are normalized to `0`
- Deduplicating insert: exact duplicate rows are skipped (existing)

## Import data into `train_stations_order_ranking`

Import from `.csv` or `.xlsx`:

```bash
python manage.py import_train_stations_order --file tables/Train_Stations_Order.xlsx
```

Canonical columns:

```text
train_num,train_station_id,train_station_order,train_station_name
```

Also supported aliases from `Train_Stations_Order.xlsx`:
- `Train_number -> train_num`
- `train_station_code -> train_station_id`
- `train_rishui_station_order_source -> train_station_order`
- `StationName -> train_station_name`

Optional command flags:

```bash
python manage.py import_train_stations_order --file path/to/data.xlsx --dry-run
python manage.py import_train_stations_order --file path/to/data.xlsx --strict
python manage.py import_train_stations_order --file path/to/data.xlsx --batch-size 500
```

Behavior:
- Deduplicating insert: exact duplicate rows are skipped (existing)
