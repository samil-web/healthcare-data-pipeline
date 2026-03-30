import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType
import sys
import os

# Import functions from the spark job (adding directory to sys.path)
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "..", "spark_jobs"))

from bronze_to_silver import sanitize_column_name, deduplicate_latest, validate_schema

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-testing") \
        .getOrCreate()

def test_sanitize_column_name():
    assert sanitize_column_name("Avg Spndg (EP)") == "avg_spndg_ep"
    assert sanitize_column_name("Facility-ID") == "facility_id"
    assert sanitize_column_name("Field Name") == "field_name"

def test_deduplicate_latest(spark):
    data = [
        ("P1", "2024-01-01 10:00:00"),
        ("P1", "2024-01-01 11:00:00"), # Latest
        ("P2", "2024-01-01 09:00:00"),
    ]
    df = spark.createDataFrame(data, ["patient_id", "ts"])
    
    dedup_df = deduplicate_latest(df, "patient_id", "ts")
    
    assert dedup_df.count() == 2
    latest_p1 = dedup_df.filter("patient_id = 'P1'").collect()[0]
    assert latest_p1["ts"] == "2024-01-01 11:00:00"

def test_validate_schema_mismatch(spark, capsys):
    expected_schema = StructType([
        StructField("id", StringType(), False),
        StructField("amount", DoubleType(), True)
    ])
    
    # Intentionally missing 'amount' and wrong type for 'id'
    data = [(1,)]
    df = spark.createDataFrame(data, ["id"])
    
    validate_schema(df, expected_schema, "TestTable")
    
    captured = capsys.readouterr()
    assert "[SCHEMA VALIDATION ERROR]" in captured.out
    assert "Missing column: amount" in captured.out

def test_validate_schema_success(spark, capsys):
    expected_schema = StructType([
        StructField("id", StringType(), False)
    ])
    
    data = [("A1",)]
    df = spark.createDataFrame(data, ["id"])
    
    validate_schema(df, expected_schema, "SuccessTable")
    
    captured = capsys.readouterr()
    assert "[SCHEMA VALIDATION SUCCESS]" in captured.out
