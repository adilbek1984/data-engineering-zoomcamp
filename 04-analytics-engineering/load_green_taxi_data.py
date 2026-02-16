import os
import sys
import urllib.request
import gzip
import shutil
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time


BUCKET_NAME = "kestra-zoomcamp-adil-demo"
CREDENTIALS_FILE = "gcs.json"

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


# -----------------------------
# Download .csv.gz
# -----------------------------
def download_file(month):
    url = f"{BASE_URL}{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"green_tripdata_2020-{month}.csv.gz")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


# -----------------------------
# Unzip .csv.gz -> .csv
# -----------------------------
def unzip_file(gz_path):
    csv_path = gz_path.replace(".gz", "")

    try:
        print(f"Unzipping {gz_path}...")
        with gzip.open(gz_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Unzipped to {csv_path}")
        return csv_path
    except Exception as e:
        print(f"Failed to unzip {gz_path}: {e}")
        return None


# -----------------------------
# Bucket creation
# -----------------------------
def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket name '{bucket_name}' is taken.")
        sys.exit(1)


# -----------------------------
# Upload CSV to GCS
# -----------------------------
def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            return
        except Exception as e:
            print(f"Upload failed: {e}")
            time.sleep(5)

    print(f"Giving up on {file_path}")


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    # 1️⃣ Download
    with ThreadPoolExecutor(max_workers=4) as executor:
        gz_files = list(executor.map(download_file, MONTHS))

    gz_files = list(filter(None, gz_files))

    # 2️⃣ Unzip
    with ThreadPoolExecutor(max_workers=4) as executor:
        csv_files = list(executor.map(unzip_file, gz_files))

    csv_files = list(filter(None, csv_files))

    # 3️⃣ Upload CSV (NOT gz)
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, csv_files)

    print("All files downloaded, unzipped, and uploaded.")
