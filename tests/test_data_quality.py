import pytest
import os
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("silver-data-quality-test") \
        .getOrCreate()

def test_silver_claims_quality(spark):
    """Assets Silver output has rows and no null primary keys."""
    path = "data/silver/hospital_claims"
    if not os.path.exists(path):
        pytest.skip(f"Silver claims data not found at {path}")
        
    df = spark.read.format("delta").load(path)
    
    # 1. Assert rows exist
    assert df.count() > 0, "Silver claims table is empty"
    
    # 2. Assert no nulls in primary key (facility_id)
    null_count = df.filter("facility_id IS NULL").count()
    assert null_count == 0, f"Found {null_count} rows with NULL facility_id"

def test_silver_patients_quality(spark):
    """Assets Silver output has rows and no null primary keys."""
    path = "data/silver/patients"
    if not os.path.exists(path):
        pytest.skip(f"Silver patients data not found at {path}")
        
    df = spark.read.format("delta").load(path)
    
    # 1. Assert rows exist
    assert df.count() > 0, "Silver patients table is empty"
    
    # 2. Assert no nulls in primary key (patient_id)
    null_count = df.filter("patient_id IS NULL").count()
    assert null_count == 0, f"Found {null_count} rows with NULL patient_id"
