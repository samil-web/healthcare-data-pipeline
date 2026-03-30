# Healthcare Data Pipeline

## Overview
This project is a healthcare data pipeline designed to process, transform, and validate healthcare data. It uses a modern data stack involving Spark for large-scale processing, Airflow for orchestration, dbt for transformations, and Great Expectations for data quality validation.

## ✨ Tech Stack
*   **Data Processing:** [PySpark](https://spark.apache.org/docs/latest/api/python/index.html)
*   **Orchestration:** [Apache Airflow](https://airflow.apache.org/)
*   **Data Transformation:** [dbt-core](https://docs.getdbt.com/)
*   **Data Validation:** [Great Expectations](https://greatexpectations.io/)
*   **Data Analysis:** [Pandas](https://pandas.pydata.org/)
*   **Testing:** [PyTest](https://docs.pytest.org/en/8.0.x/)

## 🚀 Setup

### Prerequisites
*   Python 3.11+
*   Java 8/11 (for Spark)

### Installation
1.  **Create and activate a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## 📂 Project Structure
*   `dags/`: Airflow directed acyclic graphs for pipeline orchestration.
*   `spark_jobs/`: PySpark scripts for data processing and ETL.
*   `dbt/`: dbt models and configurations for SQL transformations.
*   `tests/`: Unit and integration tests using Pytest.
*   `data/`: Sample or raw data for processing (not for production storage).
*   `config/`: Configuration files for the pipeline.

## 🧪 Testing and Validation
Run the test suite with:
```bash
pytest tests/
```
Validate data quality with Great Expectations:
```bash
great_expectations checkpoint run my_checkpoint
```

