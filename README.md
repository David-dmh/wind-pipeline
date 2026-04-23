# Wind Turbine Data Pipeline

This PySpark data pipeline system processes power output data which it cleans and analyses using data from 15 wind turbines which operate across three data groups.

---

## Overview

The pipeline reads hourly turbine readings from CSV files and performs the
following steps in sequence:

1. **Ingest** - reads raw CSV data with an explicit schema
2. **Clean** - handles missing values, invalid sensor readings
3. **Statistics** - computes daily min, max, and average power output per turbine
4. **Anomaly detection** - flags readings outside 2 standard deviations from the daily mean per turbine
5. **Write** - persists cleaned data, statistics, and anomalies to Parquet

---

## Project Structure

```
wind-pipeline/
├── data/                   # Raw input CSVs (one per turbine group)
│   ├── data_group_1.csv    # Turbines 1-5
│   ├── data_group_2.csv    # Turbines 6-10
│   └── data_group_3.csv    # Turbines 11-15
├── output/                 # Pipeline outputs (generated on run)
│   ├── cleaned/            # Cleaned data as Parquet
│   ├── stats/              # Daily summary statistics as Parquet
│   └── anomalies/          # Anomaly readings as Parquet
├── src/
│   ├── anomalies.py        # Anomaly detection logic
│   ├── clean.py            # Data cleaning and imputation
│   ├── ingest.py           # CSV ingestion with schema enforcement
│   ├── pipeline.py         # Pipeline orchestration
│   ├── spark.py            # SparkSession creator
│   ├── stats.py            # Summary statistics
│   └── write.py            # Write out data
├── tests/
│   ├── conftest.py         # Shared SparkSession fixture
│   ├── test_clean.py       # Cleaning logic tests
│   ├── test_stats.py       # Statistics logic tests
│   └── test_anomalies.py   # Anomaly detection tests
├── main.py                 # Entry point to run pipeline
└── requirements.txt        # Libraries installed via pip
```

---

## Software versions

- Python 3.13.9
- Java 17

Also see requirements.txt for libraries. 

---

## Setup

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Running the Pipeline

```bash
python main.py
```

The complete pipeline contains five steps which process data through the following stages: ingestion, cleaning, statistics, anomalies, and output generation which goes to the `output/` directory (in corresponding subfolders). 

---

## Running Tests

```bash
pytest tests/ -v
```

The command `pytest tests/ -v` initiates the test suite for execution. A common local SparkSession is created with `tests/conftest.py`. Mock dataframes are used in tests without reading the CSV files.

| File | Tests | What is covered |
|------|-------|-----------------|
| `test_clean.py` | 10 | Test deduplication, null handling, imputation, invalid wind direction NULL replacement, and removal of sensor errors such as negative power output and readings above 20 MW |
| `test_stats.py` | 4 | Test daily grouping for stats, turbine record count, column existence, and min/max/average calculations |
| `test_anomalies.py` | 3 | Test that anomaly readings are flagged using 2 standard deviation threshold. Values used are within the valid sensor range but far from the daily mean, distinguishing anomaly detection from sensor error removal which is tested separately in test_clean.py. Also verifies stable readings produce no false positives. |

---

## Pipeline Design

### Cleaning

The following steps are applied in order:

| Step | Detail |
|------|--------|
| Deduplication | Remove duplicates based on primary key |
| Invalid wind direction | If a wind direction is outside of the 0-360 degree range it will be replaced with NULL |
| Primary key null drop | Rows with null values in both `turbine_id` and `timestamp` fields are removed from the table because those fields are important identifiers which are required |
| Null imputation | The system uses per-turbine median values to fill in missing power output and wind speed data |
| Sensor error value removal | The system removes erroneous readings which show values outside the range of 0 to 20 MW or which display negative wind speed |

### Summary Statistics

The system calculates statistics for each turbine on every calendar day which runs from midnight to midnight. The output row describes one turbine which operates on one day and displays:

- `min_power` - minimum power output (MW)
- `max_power` - maximum power output (MW)
- `avg_power` - average power output (MW)
- `record_count` - extra field indicating number of readings that day. If lower than 24 indicates missing readings

### Anomaly Detection

A turbine generates power output data which is used to calculate its daily average and standard deviation. A reading is flagged as an anomaly if:

```
power_output > avg_power + (2 × std_power)
OR
power_output < avg_power - (2 × std_power)
```

Anomalies are computed per turbine per day independently. A reading that is normal for one turbine may be an anomaly for another. The anomalies output also has columns avg_power and std_power to contextualise the anomalies.

### Output Storage

Parquet was chosen for output as it's a suitable format for analytical workloads like this (queries typically scan many rows but only need a few columns) and can be previewed with IDE extensions. For production use on Databricks, Delta Lake would be ideal given the daily append pattern (see section on production considerations below).

### Dev/Prod Configuration

The pipeline supports a `base_path` argument in `write_outputs()` to separate dev and prod output locations without code changes. A production Databricks environment (see section on production considerations below) requires Declarative Automation Bundle deployment: dev and prod environments are made up of different settings, job configs and cluster settings. This would be defined in a `databricks.yml` configuration document. Sample deployment using Databricks CLI:

```bash
# Deploy to dev
databricks bundle deploy --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

### Production Considerations (Databricks)
For scalability, in Production a Databricks workspace can be used. It can serve as a single platform for ingestion and downstream modeling/analytics. The following modifications would occur:

- **Storage**: Delta Lake replaces Parquet to provide ACID transaction support, time travel and schema validation.
- **Ingestion**: Autoloader with `cloudFiles` format replaces the batch CSV read to support daily file ingestion. Autoloader uses checkpointing to track processed files therefore ingesting new data since the last run as opposed to reprocessing old files. This suits the daily append pattern.
- **Orchestration**: The local `main.py` entry point gets replaced with a Databricks job which defines the pipeline steps through dependent tasks in a DAG.
- **Compute**: The active cluster session will be reused by `setup_spark()` because it detects `DATABRICKS_RUNTIME_VERSION`.

---

## Assumptions

- **Turbine identity**: `turbine_id` is the turbine identifier and `(turbine_id, timestamp)` is treated as a composite primary key. Duplicate rows with the same pair are dropped, keeping the first occurrence. Rows where either field is NULL are dropped as they can't be uniquely identified.
- **Missing data**: Missing sensor values are assumed to be occurring randomly rather than having a given bias, therefore the median is an appropriate measure for imputation.
- **Sensor value thresholds**: I assume that power output is capped at 20 MW based on the observed data as defined in `clean.py`.
- **24-hour window**: Daily windows are calendar days, midnight to midnight. Data is assumed to be in UTC.
- **Wind direction validity**: Values outside the valid range of 0-360 degrees are set to NULL during cleaning, since standard statistical methods would be misleading in this case: e.g. a mean of 1 degree and 359 degrees is 180 degrees which is in the opposite direction to 1 and 359 degrees. Wind direction is therefore excluded from imputation, statistics, and anomaly detection.
- **CSV structure**: Each CSV always contains the same group of turbines. The pipeline reads all files in `data/` and processes them together.
- **Output data format**: The task specified using an appropriate output database. I chose to use Parquet for file-based, queryable storage. As mentioned previously I would use Delta Tables in production, so I used Parquet as it is similar to Delta. 

## Test Data Edits

`data_group_1.csv` has been modified from the original to include known edge
cases that test each cleaning and detection step:

| Row | Modification | Expected behaviour |
|-----|--------------|--------------------|
| 2-6 | Duplicate of row 1 | Dropped by deduplication |
| 12  | Missing values in one or more fields | NULL values imputed with per-turbine median |
| 19  | Negative power output | Dropped due to it being a sensor error |
| 25  | Power output above 20 MW | Dropped due to it being a sensor error |
| 39  | Unusually low power output for that turbine on that day | Flagged as anomaly |
| 47  | Unusually high power output for that turbine on that day | Flagged as anomaly |

`data_group_2.csv` and `data_group_3.csv` are unmodified from the original
data supplied.
