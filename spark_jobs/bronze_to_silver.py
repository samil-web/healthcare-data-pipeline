import os
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType
from delta import configure_spark_with_delta_pip

def create_spark_session():
    builder = SparkSession.builder \
        .appName("Healthcare-Bronze-to-Silver-Delta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

# 1. Define Expected Schemas
PATIENT_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),
    StructField("birth_date", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])

CLAIMS_SCHEMA = StructType([
    StructField("facility_id", StringType(), False),
    StructField("facility_name", StringType(), True),
    StructField("period", StringType(), True),
    StructField("standardized_date", DateType(), True),
    StructField("avg_spending_hospital", DoubleType(), True),
    StructField("avg_spending_state", DoubleType(), True),
    StructField("avg_spending_national", DoubleType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])

def sanitize_column_name(name):
    """Replaces invalid characters for Delta columns."""
    return name.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").lower()

def validate_schema(df, expected_schema, source_name):
    """Logs schema mismatches by comparing column names and types."""
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}
    
    mismatches = []
    for name, dtype in expected_fields.items():
        if name not in actual_fields:
            mismatches.append(f"Missing column: {name}")
        elif actual_fields[name] != dtype:
            mismatches.append(f"Type mismatch for {name}: Expected {dtype}, got {actual_fields[name]}")
            
    if mismatches:
        print(f"[SCHEMA VALIDATION ERROR] {source_name}: {', '.join(mismatches)}")
    else:
        print(f"[SCHEMA VALIDATION SUCCESS] {source_name} conforms to expected schema.")
    return df

def deduplicate_latest(df, partition_col, order_col):
    """Keeps latest record per entity based on timestamp."""
    window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
    return df.withColumn("row_num", F.row_number().over(window_spec)) \
             .filter("row_num = 1") \
             .drop("row_num")

def process_claims(spark, input_path, output_base):
    print(f"Processing Claims: {input_path}")
    df = spark.read.option("header", "true").csv(input_path)
    
    # Sanitize all column names first
    for col in df.columns:
        df = df.withColumnRenamed(col, sanitize_column_name(col))
    
    # 1. Transformations
    split_col = F.split(F.col("period"), " - ")
    df = df.withColumn("end_date_raw", F.expr("try_element_at(split(period, ' - '), 2)"))
    df = df.withColumn("standardized_date", F.expr("coalesce(to_date(end_date_raw, 'MM/dd/yyyy'), to_date(end_date_raw, 'yyyy-MM-dd'))"))
    
    num_cols = ["avg_spndg_per_ep_hospital", "avg_spndg_per_ep_state", "avg_spndg_per_ep_national"]
    # Rename these specifically for the schema
    df = df.withColumnRenamed("avg_spndg_per_ep_hospital", "avg_spending_hospital") \
           .withColumnRenamed("avg_spndg_per_ep_state", "avg_spending_state") \
           .withColumnRenamed("avg_spndg_per_ep_national", "avg_spending_national")
    
    final_num_cols = ["avg_spending_hospital", "avg_spending_state", "avg_spending_national"]
    for col in final_num_cols:
        df = df.withColumn(col, F.expr(f"coalesce(cast(`{col}` as double), -1.0)"))
    
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    
    # 2. Schema Validation (Asserting subset)
    validate_schema(df, CLAIMS_SCHEMA, "Claims")
    
    # 3. Deduplication (Latest per Facility)
    df = deduplicate_latest(df, "facility_id", "ingestion_timestamp")
    
    # 4. Partitioning
    df = df.withColumn("year", F.year("standardized_date")) \
           .withColumn("month", F.month("standardized_date"))
    
    output_path = os.path.join(output_base, "hospital_claims")
    df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(output_path)
    print(f"Saved Delta Claims to {output_path}")

def process_patients(spark, input_path, output_base):
    print(f"Processing Patients: {input_path}")
    df = spark.read.option("multiLine", "true").json(input_path)
    
    df = df.select(F.explode("entry").alias("ent")) \
           .where("ent.resource.resourceType = 'Patient'") \
           .withColumn("name_arr", F.from_json(F.col("ent.resource.name"), "array<struct<family:string,given:array<string>>>")) \
           .select(
                F.col("ent.resource.id").alias("patient_id"),
                F.col("ent.resource.birthDate").alias("birth_date_raw"),
                F.col("ent.resource.gender").alias("gender"),
                F.col("name_arr")[0].family.alias("last_name"),
                F.expr("array_join(name_arr[0].given, ' ')").alias("first_name")
           )
    
    df = df.withColumn("birth_date", F.to_date(F.col("birth_date_raw"), "yyyy-MM-dd"))
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    
    validate_schema(df, PATIENT_SCHEMA, "Patients")
    df = deduplicate_latest(df, "patient_id", "ingestion_timestamp")
    
    # Partition by birth year for patients
    df = df.withColumn("year", F.year("birth_date")) \
           .withColumn("month", F.month("birth_date"))
    
    output_path = os.path.join(output_base, "patients")
    df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(output_path)
    print(f"Saved Delta Patients to {output_path}")

if __name__ == "__main__":
    spark = create_spark_session()
    ds = "2026-03-29"
    bronze_base = "data/bronze"
    silver_base = "data/silver"
    
    try:
        process_claims(spark, os.path.join(bronze_base, "claims", ds, "hospital_claims_dataset.csv"), silver_base)
        process_patients(spark, os.path.join(bronze_base, "patients", ds, "synthea_sample_patient.json"), silver_base)
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    print("\nTransformation Bronze -> Silver (Delta) Completed.")
    spark.stop()
