import os
import yaml
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# Default arguments for the DAG
default_args = {
    'owner': 'healthcare_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_config():
    config_path = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'config/sources.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def ingest_source(source_name, source_format, source_path, **kwargs):
    ds = kwargs['ds']
    # Resolve path with execution date
    resolved_path = source_path.replace('{{ ds }}', ds)
    full_path = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), resolved_path)
    
    print(f"Ingesting {source_name} from {full_path}")
    
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Source file not found: {full_path}")
    
    # Store path for validation task
    kwargs['ti'].xcom_push(key='resolved_path', value=full_path)
    return f"Successfully located {source_name}"

def validate_source(source_name, source_format, **kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids=f'ingest_{source_name}', key='resolved_path')
    
    if not file_path:
        print(f"Ingestion for {source_name} likely failed or skipped. Skipping validation.")
        return
    
    print(f"Validating {source_name} at {file_path}")
    
    if source_format == 'csv':
        df = pd.read_csv(file_path, low_memory=False)
        row_count = len(df)
        cols = list(df.columns)
    elif source_format == 'json':
        with open(file_path, 'r') as f:
            data = json.load(f)
            # Basic check for ClinicalTrials.gov study format
            studies = data.get('studies', [])
            row_count = len(studies)
            cols = ['studies'] if row_count > 0 else []
    elif source_format == 'fhir_json':
        with open(file_path, 'r') as f:
            data = json.load(f)
            entries = data.get('entry', [])
            row_count = len(entries)
            cols = ['entry', 'resourceType']
    else:
        raise ValueError(f"Unknown format: {source_format}")

    # Validation Checks
    if row_count == 0:
        ti.xcom_push(key='stats', value={'source': source_name, 'rows': 0, 'nulls': 0})
        raise ValueError(f"Validation Failed: {source_name} has 0 rows.")
    
    null_count = 0
    if source_format == 'csv':
        null_count = int(pd.read_csv(file_path, low_memory=False).isnull().sum().sum())

    stats = {
        'source': source_name,
        'rows': row_count,
        'nulls': null_count,
        'timestamp': datetime.now().isoformat()
    }
    ti.xcom_push(key='stats', value=stats)
    print(f"Validation Passed: {source_name} has {row_count} records.")

def write_audit_log(**kwargs):
    ti = kwargs['ti']
    config = load_config()
    all_stats = []
    
    for source in config['sources']:
        s_name = source['name']
        stats = ti.xcom_pull(task_ids=f'validate_{s_name}', key='stats')
        if stats:
            all_stats.append(stats)
    
    if not all_stats:
        print("No stats found to write.")
        return

    audit_file = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'data/audit_log.csv')
    df_new = pd.DataFrame(all_stats)
    
    if os.path.exists(audit_file):
        df_old = pd.read_csv(audit_file)
        df_final = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_final = df_new
        
    df_final.to_csv(audit_file, index=False)
    print(f"Audit log updated at {audit_file}")

with DAG(
    'healthcare_ingestion_pipeline',
    default_args=default_args,
    description='Dynamic ingestion DAG for healthcare datasets',
    schedule='@daily',
    catchup=False,
    tags=['bronze', 'ingestion'],
) as dag:

    config = load_config()
    
    audit_log_task = PythonOperator(
        task_id='write_audit_log',
        python_callable=write_audit_log,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    for source in config['sources']:
        s_name = source['name']
        s_format = source['format']
        s_path = source['path']
        
        ingest_task = PythonOperator(
            task_id=f'ingest_{s_name}',
            python_callable=ingest_source,
            op_kwargs={
                'source_name': s_name,
                'source_format': s_format,
                'source_path': s_path
            },
        )
        
        validate_task = PythonOperator(
            task_id=f'validate_{s_name}',
            python_callable=validate_source,
            op_kwargs={
                'source_name': s_name,
                'source_format': s_format
            },
        )
        
        ingest_task >> validate_task >> audit_log_task
