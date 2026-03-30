import os
from pathlib import Path
from google.cloud import storage

# --- CONFIGURATION ---
BUCKET_NAME = "kestra-zoomcamp-adil-demo"

# Resolve paths
# Expected structure: 
# 08-course-project/upload_flights_data.py
# 08-course-project/data/flights/*.csv
current_dir = Path(__file__).parent
credentials_path = current_dir.parent / "04-analytics-engineering" / "gcs.json"
local_data_dir = current_dir / "data" / "flights"

# We will store them in a specific 'flights' folder in GCS
GCS_BASE_FOLDER = "raw/flights"

def upload_flights_to_gcs():
    """
    Scans the local flights directory and uploads all CSV files to GCS.
    Optimized for large files (flights.csv is ~600MB).
    """
    
    # 1. Check for credentials
    if not credentials_path.exists():
        print(f"[-] Error: Service account key not found at {credentials_path}")
        return

    # 2. Initialize GCS Client
    print(f"[*] Connecting to Google Cloud Storage...")
    try:
        storage_client = storage.Client.from_service_account_json(credentials_path)
        bucket = storage_client.bucket(BUCKET_NAME)
    except Exception as e:
        print(f"[-] Auth failed: {e}")
        return

    # 3. Check local directory
    if not local_data_dir.exists():
        print(f"[-] Error: Local directory {local_data_dir} not found.")
        print("[!] Please create 'data/flights/' and put airlines.csv, airports.csv, flights.csv there.")
        return

    # 4. Uploading files
    csv_files = list(local_data_dir.glob("*.csv"))
    if not csv_files:
        print(f"[-] No CSV files found in {local_data_dir}")
        return

    print(f"[*] Found {len(csv_files)} files to upload.")

    for file_path in csv_files:
        file_name = file_path.name
        # Standardizing name: lower_case and no spaces
        target_blob_name = f"{GCS_BASE_FOLDER}/{file_name.lower().replace(' ', '_')}"
        blob = bucket.blob(target_blob_name)

        print(f"[*] Uploading {file_name}...")
        try:
            # Using upload_from_filename is best for large files like flights.csv
            blob.upload_from_filename(str(file_path))
            print(f"    [OK] Uploaded to gs://{BUCKET_NAME}/{target_blob_name}")
        except Exception as e:
            print(f"    [X] Failed to upload {file_name}: {e}")

    print("\n[V] Ingestion process finished.")

if __name__ == "__main__":
    upload_flights_to_gcs()