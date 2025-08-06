from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, upper, lower, title, trim, lit, concat_ws
from pyspark.sql.types import *
import json
import io
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

def get_minio_client():
    """Get MinIO client configuration"""
    # Get MinIO configuration from environment variables (required)
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')
    minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    
    # Validate required environment variables
    if not all([minio_endpoint, minio_access_key, minio_secret_key]):
        raise ValueError("Missing required MinIO environment variables: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY")
    
    return Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure
    )

def get_latest_bronze_file(**context):
    """Get the latest file from bronze bucket"""
    client = get_minio_client()
    bucket_name = "bronze"
    
    try:
        # Check if bucket exists
        if not client.bucket_exists(bucket_name):
            raise ValueError(f"Bronze bucket '{bucket_name}' does not exist")
        
        objects = list(client.list_objects(bucket_name, prefix="open_brewery_db_"))
        
        if not objects:
            raise ValueError(f"No files found in bronze bucket with prefix 'open_brewery_db_'")
        
        logging.info(f"Found {len(objects)} bronze files")
        latest_file = None
        latest_time = None
        
        for obj in objects:
            if latest_time is None or obj.last_modified > latest_time:
                latest_time = obj.last_modified
                latest_file = obj.object_name
        
        if latest_file is None:
            raise ValueError("No bronze files found")
            
        logging.info(f"Latest bronze file: {latest_file}")
        context['ti'].xcom_push(key='bronze_file', value=latest_file)
        return latest_file
        
    except Exception as e:
        raise RuntimeError(f"Error finding latest bronze file: {e}")

def transform_to_silver(**context):
    """Transform bronze data to silver layer with data quality improvements and partitioning"""
    bronze_file = context['ti'].xcom_pull(key='bronze_file', task_ids='get_latest_bronze_file')
    if not bronze_file:
        raise ValueError("No bronze file provided")
    
    client = get_minio_client()
    
    # Initialize Spark session with production-ready configuration
    spark = SparkSession.builder \
        .appName(f"BrewerySilverTransformation_{context['ts_nodash']}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200")) \
        .getOrCreate()
    
    try:
        # Read bronze data
        logging.info(f"Reading bronze file: {bronze_file}")
        response = client.get_object("bronze", bronze_file)
        raw_data = json.loads(response.data.decode('utf-8'))
        
        # Validate raw data
        if not raw_data or not isinstance(raw_data, list):
            raise ValueError(f"Invalid or empty data in bronze file: {bronze_file}")
        
        # Create Spark DataFrame
        df = spark.createDataFrame(raw_data)
        
        record_count = df.count()
        if record_count == 0:
            raise ValueError(f"No records found in bronze file: {bronze_file}")
            
        logging.info(f"Loaded {record_count} records from bronze layer")
        
        # Data transformations and cleaning
        
        # Clean and standardize data
        df = df.withColumn('brewery_type', 
                          lower(trim(when(col('brewery_type').isNull(), 'unknown')
                                   .otherwise(col('brewery_type')))))
        df = df.withColumn('country', 
                          upper(trim(when(col('country').isNull(), 'unknown')
                                    .otherwise(col('country')))))
        df = df.withColumn('state', 
                          upper(trim(when(col('state').isNull(), 'unknown')
                                    .otherwise(col('state')))))
        df = df.withColumn('city', 
                          trim(when(col('city').isNull(), 'unknown')
                              .otherwise(col('city'))))
        
        # Create location hierarchy for partitioning
        df = df.withColumn('location', 
                          concat_ws('/', col('country'), col('state'), col('city')))
        
        # Clean numeric fields
        df = df.withColumn('latitude', 
                          col('latitude').cast('double'))
        df = df.withColumn('longitude', 
                          col('longitude').cast('double'))
        
        # Add data quality flags
        df = df.withColumn('has_coordinates', 
                          ~(col('latitude').isNull() | col('longitude').isNull()))
        df = df.withColumn('has_website', 
                          ~col('website_url').isNull())
        df = df.withColumn('has_phone', 
                          ~col('phone').isNull())
        
        # Add processing metadata
        df = df.withColumn('processed_at', lit(datetime.now().isoformat()))
        df = df.withColumn('data_source', lit('open_brewery_db_api'))
        
        # Create silver bucket if it doesn't exist
        silver_bucket = "silver"
        if not client.bucket_exists(silver_bucket):
            client.make_bucket(silver_bucket)
        
        # Partition by location and save as parquet
        partitions_created = []
        locations = df.select('location').distinct().collect()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for location_row in locations:
            location = location_row['location']
            # Clean location path for file system
            safe_location = location.replace('/', '_').replace(' ', '_').lower()
            
            # Filter data for this location
            location_df = df.filter(col('location') == location)
            record_count = location_df.count()
            
            # Convert to parquet using temp file
            temp_dir = os.getenv("AIRFLOW_TEMP_DIR", "/tmp")
            temp_parquet = os.path.join(temp_dir, f"{safe_location}_{timestamp}_{context['ts_nodash']}")
            os.makedirs(temp_parquet, exist_ok=True)
            location_df.coalesce(1).write.mode('overwrite').parquet(temp_parquet)
            
            # Read the single parquet file and upload to MinIO
            import glob
            parquet_files = glob.glob(f"{temp_parquet}/part-*.parquet")
            if parquet_files:
                with open(parquet_files[0], 'rb') as f:
                    parquet_data = f.read()
                
                filename = f"breweries_by_location/{safe_location}/breweries_{timestamp}.parquet"
                parquet_buffer = io.BytesIO(parquet_data)
                
                # Store in MinIO
                client.put_object(
                    silver_bucket,
                    filename,
                    parquet_buffer,
                    length=len(parquet_data),
                    content_type="application/octet-stream"
                )
                
                partitions_created.append({
                    'location': location, 
                    'filename': filename, 
                    'record_count': record_count
                })
                logging.info(f"Created partition: {filename} with {record_count} records")
                
                # Clean up temp parquet directory
                import shutil
                shutil.rmtree(temp_parquet, ignore_errors=True)
        
        # Store metadata about partitions created
        context['ti'].xcom_push(key='partitions_created', value=partitions_created)
        context['ti'].xcom_push(key='total_records', value=record_count)
        
        logging.info(f"Silver layer transformation completed. Created {len(partitions_created)} partitions with {record_count} total records")
        
    except S3Error as e:
        raise RuntimeError(f"MinIO S3 error during silver transformation: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error during silver transformation: {e}")
    finally:
        # Clean up Spark session
        if 'spark' in locals():
            spark.stop()

with DAG(
    'medallion_silver_layer',
    default_args=default_args,
    description='Silver Layer: Transform and partition brewery data by location in parquet format',
    schedule=None,  # Triggered by bronze layer completion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'silver', 'transformation', 'etl'],
) as dag:

    get_bronze_file = PythonOperator(
        task_id='get_latest_bronze_file',
        python_callable=get_latest_bronze_file,
    )

    transform_data = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
    )

    get_bronze_file >> transform_data