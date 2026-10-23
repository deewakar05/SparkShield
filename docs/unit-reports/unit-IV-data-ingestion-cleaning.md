# SparkShield — Unit IV Report: Dataset Ingestion and Data Cleaning
*Milestone 2 | Completed: 2026-10-24*

---

## 1. Concepts Demonstrated

### Unit IV — Spark SQL, DataFrames & Data Cleaning
| Concept | Location | Implementation Details |
|---|---|---|
| Distributed CSV Parsing | [`DataIngestion.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataIngestion.scala) | Uses Spark DataFrame reader with `header=true`, `mode=PERMISSIVE`, `inferSchema=false` without collecting data to driver |
| Column Normalization | [`DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Rule C1: Trims whitespace, lowercases, replaces non-alphanumerics with underscores, strips edge underscores |
| Deterministic Duplicate Resolution | [`DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Rule C2: Identifies duplicate fields (including Spark SafeHeader index suffixes on col 35 & 56 `Fwd Header Length`), appending `_2`, `_3` |
| Safe Positional Renaming | [`DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Uses `df.toDF(renamedCols: _*)` to safely rename duplicate columns without triggering Spark `AMBIGUOUS_REFERENCE` errors |
| Numeric Type Casting | [`DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Rule C4: Explicit distributed casting of 78 feature columns to `DoubleType`, avoiding silent zero coercion |
| Label Standardisation | [`DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Rule C3: Case-insensitive target column discovery; preserves trimmed raw label in `label_original`, derives binary `label_binary` (`BENIGN` vs `ATTACK`) |
| Invalid Numeric Flagging | [`DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Rule C5: Identifies `Infinity`, `-Infinity`, and `NaN` without dropping rows; flags via boolean column `has_invalid_numeric` |
| Distributed Metric Aggregation | [`DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Uses single-pass distributed `count`, `agg(sum(col.isNull))`, and `groupBy` without collecting full dataset |
| Optimized Storage Output | [`DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Writes partitioned columnar Parquet to `data/processed/`, enforcing explicit error-if-exists mode to prevent accidental overwrites |

---

## 2. Source Files Involved

| File | Purpose |
|---|---|
| [`scala/src/main/scala/sparkshield/DataIngestion.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataIngestion.scala) | Distributed CSV DataFrame reader (single file, sampled, and multi-file directory modes) |
| [`scala/src/main/scala/sparkshield/DataCleaning.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/DataCleaning.scala) | Production data cleaning pipeline with deterministic duplicate resolution and metric reporting |
| [`scala/src/test/scala/sparkshield/DataCleaningSpec.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/test/scala/sparkshield/DataCleaningSpec.scala) | 19 unit tests covering normalization, duplicates, label handling, numeric casting, invalid flagging, nulls, and schema consistency |
| [`scripts/clean-dataset.sh`](file:///Users/deewakarkumar/Projects/SparkShield/scripts/clean-dataset.sh) | Executable automation script for test, inspect, clean, and batch cleaning operations |
| [`scripts/run.sh`](file:///Users/deewakarkumar/Projects/SparkShield/scripts/run.sh) | Updated spark-submit runner supporting both Spark options and application CLI arguments |
| [`results/metrics/data-quality-report.json`](file:///Users/deewakarkumar/Projects/SparkShield/results/metrics/data-quality-report.json) | Machine-readable data quality report produced directly from real dataset execution |

---

## 3. Dataset Inventory (CICIDS2017)

All 8 files located in `/Users/deewakarkumar/Downloads/MachineLearningCVE`:

| File Name | File Size | Columns | Tested Status |
|---|---|---|---|
| `Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv` | 74 MB | 79 | **Fully Ingested, Cleaned & Verified** |
| `Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv` | 73 MB | 79 | Verified schema header |
| `Friday-WorkingHours-Morning.pcap_ISCX.csv` | 56 MB | 79 | Verified schema header |
| `Monday-WorkingHours.pcap_ISCX.csv` | 169 MB | 79 | Verified schema header & row count |
| `Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv` | 79 MB | 79 | Verified schema header |
| `Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv` | 50 MB | 79 | Verified schema header (contains UTF-8 dash) |
| `Tuesday-WorkingHours.pcap_ISCX.csv` | 129 MB | 79 | Verified schema header |
| `Wednesday-workingHours.pcap_ISCX.csv` | 215 MB | 79 | Verified schema header |

---

## 4. Exact Execution Commands

### 1. Run Unit Tests (All 33 Tests)
```bash
cd /Users/deewakarkumar/Projects/SparkShield/scala
../scripts/sbt.sh test
# Or using the helper script from project root:
./scripts/clean-dataset.sh test
```

### 2. Inspect One CSV File (Distributed Header, Sample, Row Count, Labels)
```bash
./scripts/run.sh sparkshield.DataIngestion /Users/deewakarkumar/Downloads/MachineLearningCVE/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv
# Or via helper:
./scripts/clean-dataset.sh inspect /Users/deewakarkumar/Downloads/MachineLearningCVE/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv
```

### 3. Clean One CSV File and Generate Parquet + Quality Report
```bash
./scripts/run.sh sparkshield.DataCleaning \
  /Users/deewakarkumar/Downloads/MachineLearningCVE/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv \
  data/processed/cleaned_2026-10-24
# Or via helper:
./scripts/clean-dataset.sh clean \
  /Users/deewakarkumar/Downloads/MachineLearningCVE/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv \
  data/processed/cleaned_2026-10-24
```

### 4. Validate Parquet Output
```bash
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').appName('verify').getOrCreate()
df = spark.read.parquet('data/processed/cleaned_2026-10-24')
print('Rows:', df.count())
print('Columns:', len(df.columns))
print('Columns list sample:', df.columns[:5], df.columns[-5:])
spark.stop()
"
```

### 5. Run the Pipeline on All Eight Files (Batch Mode)
```bash
./scripts/clean-dataset.sh all \
  /Users/deewakarkumar/Downloads/MachineLearningCVE \
  data/processed/all_cleaned_2026-10-24
```

---

## 5. Measured Data Quality & Verification Results

> The following metrics are **measured values** directly produced by running `sparkshield.DataCleaning` against `/Users/deewakarkumar/Downloads/MachineLearningCVE/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv`.

### A. Execution Summary
- **Input File**: `Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv`
- **Output Path**: `data/processed/cleaned_2026-10-24`
- **Total Rows Measured**: `225,745`
- **Raw Columns Count**: `79`
- **Clean Columns Count**: `81`
- **Rows with Invalid Numerics (`Infinity`/`NaN`)**: `34` (0.0151%)
- **Null Value Counts**: `0` across all columns in this file

### B. Duplicate Column Resolution
- In the raw CSV header, column index 34 and column index 55 (0-indexed) are both named `" Fwd Header Length"`.
- When Spark parses the CSV with headers, Univocity safe-header generation produces `" Fwd Header Length34"` and `" Fwd Header Length55"`.
- `DataCleaning.buildSchema` identifies the common base name `" Fwd Header Length"`, deterministically renaming:
  - First occurrence (col 35) $\rightarrow$ `fwd_header_length`
  - Second occurrence (col 56) $\rightarrow$ `fwd_header_length_2`
- Recorded mapping in `duplicatesResolved`:
  ```json
  "duplicate_columns_resolved": {
    "Fwd Header Length": "fwd_header_length_2"
  }
  ```

### C. Label Distribution
| Original Label (`label_original`) | Record Count | Percentage | Binary Label (`label_binary`) |
|---|---|---|---|
| `DDoS` | 128,027 | 56.71% | `ATTACK` |
| `BENIGN` | 97,718 | 43.29% | `BENIGN` |
| **Total** | **225,745** | **100.00%** | — |

### D. Clean Schema Structure (81 Columns Total)
The clean DataFrame contains:
- 78 feature columns cast to `DoubleType` (e.g., `destination_port`, `flow_duration`, `flow_bytes_s`, `fwd_header_length`, `fwd_header_length_2`, `subflow_fwd_packets`, `init_win_bytes_forward`, etc.)
- `label_original`: `StringType` (trimmed raw label, e.g. `"DDoS"`, `"BENIGN"`)
- `label_binary`: `StringType` (`"BENIGN"` vs `"ATTACK"`)
- `has_invalid_numeric`: `BooleanType` (`true` if any numeric feature in that row is `Infinity` or `NaN`)

---

## 6. Documented Cleaning Decisions (Rules C1–C6)

1. **C1 (Column Normalization)**:
   - Trim leading and trailing whitespace.
   - Convert to lowercase.
   - Replace any sequence of non-alphanumeric characters with `_`.
   - Strip leading and trailing underscores.
2. **C2 (Duplicate Resolution)**:
   - Base duplicate occurrences get `_2`, `_3`, etc. in order of column appearance.
   - Specifically resolves the repeated `Fwd Header Length` field at columns 35 and 56 to `fwd_header_length` and `fwd_header_length_2`.
   - Safe positional renaming via `df.toDF(normalizedColumns: _*)` prevents Spark SQL ambiguous reference resolution errors.
3. **C3 (Label Normalization & Binary Target)**:
   - Target column `Label` is matched case-insensitively with whitespace trimmed.
   - Preserves original attack type in `label_original`.
   - Derives binary label `label_binary`: `BENIGN` for benign traffic, `ATTACK` for all intrusion flows.
4. **C4 (Numeric Casting)**:
   - All 78 feature columns are explicitly cast from `StringType` to `DoubleType`.
   - String `"Infinity"` casts to `Double.PositiveInfinity`, preserving the extreme flow rate information.
   - Parsing failures yield `null` rather than silently converting to `0.0`.
5. **C5 (Invalid Value Handling)**:
   - Rows are **never dropped silently**.
   - Rows containing `Double.PositiveInfinity`, `Double.NegativeInfinity`, or `Double.NaN` in any numeric feature are tagged with `has_invalid_numeric = true`.
   - Allows downstream ML and SQL models to explicitly filter or impute without losing raw data visibility.
6. **C6 (Null Handling)**:
   - Nulls resulting from malformed input in `PERMISSIVE` mode are tracked and counted per column in a single distributed aggregation pass.

---

## 7. Limitations & Unresolved Data Issues

1. **Mojibake in Attack Labels**:
   - In `Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv`, the label contains UTF-8 en-dash bytes (`\xe2\x80\x93`) rendered as `Web Attack â Brute Force`, `Web Attack â XSS`, and `Web Attack â Sql Injection`.
   - In downstream feature engineering / ML, a regex or UTF-8 decoding pass should normalize `â` to `-`.
2. **Infinite Flow Rates**:
   - `flow_bytes_s` and `flow_packets_s` contain `"Infinity"` when flow duration is near zero ($0$ microseconds).
   - In Milestone 2, these are preserved as `Double.PositiveInfinity` and tagged in `has_invalid_numeric`. For ML training in Milestone 5, winsorization or median imputation will be required.
3. **Memory Consideration for Batch Processing**:
   - Each raw CSV ranges from 50 MB to 215 MB (totaling ~1 GB).
   - In batch mode (`clean-dataset.sh all`), files should be processed one by one rather than unioned in memory simultaneously on 8 GB laptops, writing each to its own Parquet partition.

---

## 8. Verification Sign-Off

- [x] All 14 Milestone 1 unit tests (`RDDBasicsSpec`) pass.
- [x] All 19 Milestone 2 unit tests (`DataCleaningSpec`) pass.
- [x] Total 33 unit tests pass without failure.
- [x] Zero compilation warnings (`-target:jvm-1.8`, `-Xlint`).
- [x] Assembly fat jar built successfully (`sparkshield-assembly-1.0.0.jar`).
- [x] Real CSV file (`Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv`) ingested, cleaned, and verified.
- [x] Parquet output generated in `data/processed/cleaned_2026-10-24` and verified.
- [x] Machine-readable report saved in `results/metrics/data-quality-report.json`.
- [x] Script `scripts/clean-dataset.sh` tested and documented.
