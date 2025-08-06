from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import io
import time
from minio import Minio
from minio.error import S3Error
import os
import logging

default_args = {
    'owner': 'Gabriel Augusto Rosa',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def fetch_all_breweries(**context):
    """Fetch all breweries using pagination from Open Brewery DB API"""
    base_url = "https://api.openbrewerydb.org/v1/breweries"
    all_breweries = []
    page = 1
    per_page = 200  # Maximum allowed per page
    max_pages = 100  # Safety limit to prevent infinite loops
    
    logging.info(f"Starting brewery data fetch from {base_url}")

    while page <= max_pages:
        params = {'page': page, 'per_page': per_page}
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not data:
                logging.info(f"No more data found at page {page}. Stopping pagination.")
                break
            all_breweries.extend(data)
            logging.info(f"Fetched {len(data)} records from page {page}. Total: {len(all_breweries)}")
            page += 1
            time.sleep(0.1)  # Be respectful to the API
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching page {page}: {e}")
            raise RuntimeError(f"Error fetching page {page}: {e}")
    
    if page > max_pages:
        logging.warning(f"Reached maximum page limit ({max_pages}). There might be more data available.")
    
    logging.info(f"Total breweries fetched: {len(all_breweries)}")
    
    if not all_breweries:
        raise ValueError("No brewery data was fetched from the API")
    
    # Store count in XCom for monitoring
    context['ti'].xcom_push(key='brewery_count', value=len(all_breweries))
    # Push data to XCom (consider file storage for very large datasets)
    context['ti'].xcom_push(key='breweries_json', value=json.dumps(all_breweries))

def store_in_minio(**context):
    """Store JSON data in MinIO bronze bucket"""
    logging.info("Starting MinIO storage process")
    
    breweries_json = context['ti'].xcom_pull(key='breweries_json', task_ids='fetch_breweries')
    brewery_count = context['ti'].xcom_pull(key='brewery_count', task_ids='fetch_breweries')
    
    if not breweries_json:
        raise ValueError("No brewery data received from previous task")
    
    data = json.loads(breweries_json)
    logging.info(f"Processing {len(data)} brewery records for storage")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"open_brewery_db_{timestamp}.json"

    # Get MinIO configuration from environment variables (required)
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')
    minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    
    # Validate required environment variables
    if not all([minio_endpoint, minio_access_key, minio_secret_key]):
        raise ValueError("Missing required MinIO environment variables: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY")
    
    logging.info(f"Connecting to MinIO at {minio_endpoint}")

    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure
    )

    bucket_name = "bronze"

    try:
        if not client.bucket_exists(bucket_name):
            logging.info(f"Creating bucket '{bucket_name}'")
            client.make_bucket(bucket_name)
        
        json_data = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        data_stream = io.BytesIO(json_data)
        file_size_mb = len(json_data) / (1024 * 1024)
        
        logging.info(f"Uploading {filename} ({file_size_mb:.2f} MB) to bucket '{bucket_name}'")
        
        client.put_object(
            bucket_name,
            filename,
            data_stream,
            length=len(json_data),
            content_type="application/json"
        )
        
        logging.info(f"Successfully stored {len(data)} brewery records in {filename}")
    except S3Error as e:
        logging.error(f"MinIO S3 error during upload: {e}")
        raise RuntimeError(f"MinIO S3 error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during MinIO upload: {e}")
        raise RuntimeError(f"Unexpected error: {e}")

with DAG(
    'medallion_bronze_layer',
    default_args=default_args,
    description='Bronze Layer: Fetch breweries from Open Brewery DB and store raw data in MinIO',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'bronze', 'brewery', 'etl'],
) as dag:

    fetch_breweries = PythonOperator(
        task_id='fetch_breweries',
        python_callable=fetch_all_breweries,
    )

    store_breweries = PythonOperator(
        task_id='store_in_minio',
        python_callable=store_in_minio,
    )

    fetch_breweries >> store_breweries