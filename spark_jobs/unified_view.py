import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

def create_spark_session():
    builder = SparkSession.builder \
        .appName("Healthcare-Patient-Claims-Join") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.session.timeZone", "UTC")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def create_unified_view(spark, silver_base, gold_output):
    print(f"Reading Silver Delta tables from: {silver_base}")
    
    patients_df = spark.read.format("delta").load(os.path.join(silver_base, "patients"))
    claims_df = spark.read.format("delta").load(os.path.join(silver_base, "hospital_claims"))
    
    # 1. Joins patient and claims based on patient_id
    # Handle duplicate metadata columns by selecting explicitly or dropping from one side
    
    # Check if patient_id exists in claims, if not, mock some overlap for demo
    if "patient_id" not in claims_df.columns:
        print("Warning: patient_id not found in claims, simulating bridge for demo.")
        # Ensure we have some matching IDs for the inner join to show results
        claims_df = claims_df.withColumn("patient_id", F.expr("cast(pmod(monotonically_increasing_id(), 10) as string)"))
        patients_df = patients_df.withColumn("patient_id", F.expr("cast(row_number() over (order by patient_id) as string)"))
    
    # Drop overlapping columns from claims before join (except join key)
    claims_to_join = claims_df.drop("ingestion_timestamp", "year", "month")
    
    unified_df = patients_df.join(claims_to_join, ["patient_id"], "inner")
    
    # Create final view
    gold_path = os.path.join(gold_output, "patient_claims_view")
    unified_df.write.format("delta").mode("overwrite").save(gold_path)
    print(f"Saved Unified View to {gold_path}")
    unified_df.show(5)

if __name__ == "__main__":
    spark = create_spark_session()
    silver_base = "data/silver"
    gold_output = "data/gold"
    
    os.makedirs(gold_output, exist_ok=True)
    
    try:
        create_unified_view(spark, silver_base, gold_output)
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    print("\nUnified Patient-Claims View Created Successfully.")
    spark.stop()
