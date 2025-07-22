import io
from pymongo import MongoClient
import pandas as pd
import json
from utils import mongodb_connect
from google.cloud import storage
import logging
import re

logging.basicConfig(level=logging.INFO, filename="pipeline.log",
                    format='%(asctime)s - %(levelname)s - %(message)s')

def extract_batches(collection, batch_size=1000):
    total_docs = collection.count_documents({})
    for i in range(0, total_docs, batch_size):
        yield list(collection.find().skip(i).limit(batch_size))

def convert_to_format(data, format_type='json'):
    if format_type == 'json':
        return json.dumps(data, default=str)
    elif format_type == 'csv':
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    elif format_type == 'parquet':
        df = pd.DataFrame(data)
        # Convert _id to string safely
        if '_id' in df.columns:
            df['_id'] = df['_id'].apply(lambda x: str(x) if x is not None else "")

        # Replace NaN/None in all object-type columns
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].fillna("").astype(str)
            
        # Write to in-memory buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        buffer.seek(0)
        return buffer.read()


def upload_to_gcs(bucket_name, blob_name, content):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content)


def log_status(message):
    logging.info(message)


def get_uploaded_batch_indexes(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    indexes = []
    for blob in blobs:
        match = re.search(r'batch_(\d+)', blob.name)
        if match:
            indexes.append(int(match.group(1)))
    return set(indexes)

def export_to_gcs(connection, db, collection, path):
    client, db, collection = mongodb_connect(connection, db, collection)

    # This is an update to handle the uploaded files due to an interruption on the previous upload
    uploaded_batches = get_uploaded_batch_indexes("glamira-raw-data", path)
    
    batch_size = 1000
    total_docs = collection.count_documents({})

    for i in range(0, total_docs, batch_size):
        batch_index = i // batch_size
        # Due to the big amount of data and the potential of interruption, this check is to skip the uploaded files and continue with the new data
        if batch_index in uploaded_batches:
            print(f"Skipping batch {batch_index} (already uploaded)")
            continue

        batch = list(collection.find().sort("_id").skip(i).limit(batch_size))
        if not batch:
            break

        _data = convert_to_format(batch, format_type='parquet')
        filename = f"{path}/batch_{batch_index}.parquet"
        
        try:
            upload_to_gcs("glamira-raw-data", filename, _data)
            log_status(f"Uploaded batch {batch_index} to GCS.")
            print(f"Uploaded batch {batch_index}, docs: {len(batch)}")
        except Exception as e:
            log_status(f"Upload failed for batch {batch_index}: {e}")
            print(f"Upload failed: {e}")


connection = "mongodb://localhost:27017/"
db = "glamira"
collection = "summary"
ip_collection = "location"
path_summary = "summary_users"
path_ip2location = "ip2location"
export_to_gcs(connection, db, collection, path_summary)
