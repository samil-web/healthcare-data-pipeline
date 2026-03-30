import pandas as pd
import json
import os
from datetime import datetime

def profile_csv(file_path):
    print(f"\n--- Profiling CSV: {os.path.basename(file_path)} ---")
    df = pd.read_csv(file_path, low_memory=False)
    print(f"Row count: {len(df)}")
    print("\nNull Percentage (%):")
    print((df.isnull().sum() / len(df) * 100).sort_values(ascending=False).head(10))
    print("\nData Types:")
    print(df.dtypes.head(10))
    print("\nUnique Values (First 5 columns):")
    for col in df.columns[:5]:
        print(f"{col}: {df[col].nunique()}")

def profile_json_clinical_trials(file_path):
    print(f"\n--- Profiling Clinical Trials JSON: {os.path.basename(file_path)} ---")
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    studies = data.get('studies', [])
    print(f"Study count: {len(studies)}")
    
    # Flatten basic info
    flat_data = []
    for s in studies:
        info = s.get('protocolSection', {})
        id_info = info.get('identificationModule', {})
        status_info = info.get('statusModule', {})
        flat_data.append({
            'nct_id': id_info.get('nctId'),
            'title': id_info.get('briefTitle'),
            'status': status_info.get('overallStatus'),
            'start_date': status_info.get('startDateStruct', {}).get('date')
        })
    
    df = pd.DataFrame(flat_data)
    print(df.info())
    print("\nNull %:")
    print(df.isnull().sum() / len(df) * 100)

def profile_fhir_json(file_path):
    print(f"\n--- Profiling FHIR JSON (Synthea): {os.path.basename(file_path)} ---")
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    entries = data.get('entry', [])
    print(f"Resource count: {len(entries)}")
    
    resource_types = {}
    for entry in entries:
        rtype = entry.get('resource', {}).get('resourceType')
        resource_types[rtype] = resource_types.get(rtype, 0) + 1
    
    print("\nResource Type Distribution:")
    for rt, count in resource_types.items():
        print(f"{rt}: {count}")

if __name__ == "__main__":
    base_dir = "data/bronze"
    date_str = "2026-03-29"
    
    # Claims
    claims_path = os.path.join(base_dir, "claims", date_str, "hospital_claims_dataset.csv")
    if os.path.exists(claims_path):
        profile_csv(claims_path)
    
    # Trials
    trials_path = os.path.join(base_dir, "trials", date_str, "diabetes_clinical_trials.json")
    if os.path.exists(trials_path):
        profile_json_clinical_trials(trials_path)
        
    # Patients
    patients_path = os.path.join(base_dir, "patients", date_str, "synthea_sample_patient.json")
    if os.path.exists(patients_path):
        profile_fhir_json(patients_path)
