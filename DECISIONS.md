# Design Decisions (DECISIONS.md)

This document outlines critical architectural and technical decisions made during the development of the Healthcare Data Pipeline.

### 1. Choice of Parquet & Delta Lake for Silver Storage
*   **Decision:** All processed data is stored in **Delta Lake** (Parquet) format, partitioned by `year` and `month`.
*   **Rationale:** Standard Parquet provides efficient storage and columnar compression, but Delta Lake adds **ACID transactions** and schema enforcement. Partitioning by date as a "SCD Type 1" style approach ensures high-performance retrieval for the periodic claims and clinical trials data, which are typically queried by time range.

### 2. Standardisation via SQL Expressions (Spark `F.expr`)
*   **Decision:** Used `F.expr` and `F.lit` instead of purely native Python/Spark wrappers for complex date parsing and null handling.
*   **Rationale:** In local development environments with heterogeneous PySpark versions, native API boundaries (like `F.coalesce` with Python lists) can trigger `[NOT_ITERABLE]` errors. Using the Spark SQL engine via `expr` ensures the logic stays within the JVM/Spark native core, increasing cross-environment stability.

### 3. Integrated dbt-DuckDB for Gold Layer Analytics
*   **Decision:** Orchestrated transformed Parquet files into **DuckDB** using **dbt**.
*   **Rationale:** While Spark is excellent for processing large datasets in the Bronze/Silver layers, dbt provides a superior framework for **Analytics Engineering**. DuckDB was chosen for the local "Gold" layer as it allows running modern SQL (including Delta reading) with ACID properties on a local machine without a heavy server requirement (mimicking a cloud Data Warehouse experience).

### 4. Implementation of "Sentinel" Values for Null Handling
*   **Decision:** Filled numeric nulls with `-1.0` and categorized categorical nulls as `Unknown`.
*   **Rationale:** In healthcare datasets (CMS Claims), a `NULL` spending value might mean "data missing" or "no spending". Re-mapping to a sentinel value ensures that downstream ML models and aggregate reports don't implicitly drop rows, while still allowing analysts to filter out "Invalid/Unknown" data easily.

### 5. Shift to Staging Views for Source Isolation
*   **Decision:** Created `stg_` models in dbt to wrap raw Parquet files.
*   **Rationale:** By creating a staging layer, we isolate the analytical "Gold" models from changes in the raw Parquet structure. If the Bronze ingestion logic changes (e.g., column renaming), only one staging model needs an update, rather than all downstream analytics.
