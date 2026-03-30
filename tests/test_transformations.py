import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import sys
import os

# Ensure spark_jobs is in path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))

from bronze_to_silver import sanitize_column_name, deduplicate_latest

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("transformation-unit-tests") \
        .getOrCreate()

def test_sanitize_column_name():
    """Test that CSV headers are converted to clean snake_case."""
    assert sanitize_column_name("Facility Name") == "facility_name"
    assert sanitize_column_name("Avg Spndg (EP)") == "avg_spndg_ep"
    assert sanitize_column_name("Claim-Type") == "claim_type"

def test_deduplicate_latest(spark):
    """Test that only the latest record is kept per ID."""
    data = [
        ("ID1", "2024-01-01 10:00:00"),
        ("ID1", "2024-01-01 12:00:00"), # Latest
        ("ID2", "2024-01-01 09:00:00")
    ]
    df = spark.createDataFrame(data, ["id", "ingestion_timestamp"])
    
    result_df = deduplicate_latest(df, "id", "ingestion_timestamp")
    
    assert result_df.count() == 2
    latest_id1 = result_df.filter("id = 'ID1'").collect()[0]
    assert latest_id1["ingestion_timestamp"] == "2024-01-01 12:00:00"

def test_date_standardization_expr(spark):
    """Test that date strings are standardized to ISO 8601 using Spark SQL expr."""
    data = [("10/01/2023"), ("2023-10-01"), ("Invalid Date")]
    df = spark.createDataFrame([(d,) for d in data], ["raw_date"])
    
    # Use the same expression logic from bronze_to_silver.py
    df = df.withColumn("std_date", F.expr("coalesce(to_date(raw_date, 'MM/dd/yyyy'), to_date(raw_date, 'yyyy-MM-dd'))"))
    
    results = df.collect()
    assert str(results[0]["std_date"]) == "2023-10-01"
    assert str(results[1]["std_date"]) == "2023-10-01"
    assert results[2]["std_date"] is None

def test_null_handling_expr(spark):
    """Test that null numeric columns are filled with -1.0 sentinel."""
    data = [(10.5,), (None,)]
    df = spark.createDataFrame(data, ["spending"])
    
    # Use the logic from bronze_to_silver.py
    df = df.withColumn("spending", F.expr("coalesce(cast(spending as double), -1.0)"))
    
    results = df.collect()
    assert results[0]["spending"] == 10.5
    assert results[1]["spending"] == -1.0
