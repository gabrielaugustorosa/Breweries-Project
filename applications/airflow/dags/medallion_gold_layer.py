from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, round as spark_round, when, isnan, isnull
from pyspark.sql.types import *
import json
import io
from minio import Minio
from minio.error import S3Error
import os
import logging
from production_utils import (
    get_spark_session, validate_environment_variables, log_execution_metrics,
    retry_with_exponential_backoff, cleanup_temp_files, get_temp_directory,
    validate_data_quality, REQUIRED_MINIO_VARS, PRODUCTION_TIMEOUTS
)

default_args = {
    'owner': 'Gabriel Augusto Rosa',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

@retry_with_exponential_backoff(max_retries=3)
def get_minio_client():
    """Get MinIO client configuration with production error handling"""
    env_vars = validate_environment_variables(REQUIRED_MINIO_VARS)
    minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    
    try:
        client = Minio(
            env_vars['MINIO_ENDPOINT'],
            access_key=env_vars['MINIO_ACCESS_KEY'],
            secret_key=env_vars['MINIO_SECRET_KEY'],
            secure=minio_secure
        )
        
        # Test connection
        client.list_buckets()
        logging.info(f"Successfully connected to MinIO at {env_vars['MINIO_ENDPOINT']}")
        return client
        
    except Exception as e:
        logging.error(f"Failed to connect to MinIO: {e}")
        raise RuntimeError(f"MinIO connection failed: {e}")

@log_execution_metrics
def aggregate_brewery_data(**context):
    """Create analytical aggregations for gold layer with production error handling"""
    logging.info("Starting gold layer aggregation process")
    
    client = get_minio_client()
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")
    gold_bucket = os.getenv("GOLD_BUCKET", "gold")
    temp_files = []
    temp_dirs = []
    
    with get_spark_session("BreweryGoldAggregation", context) as spark:
    
    try:
        # Check if silver bucket exists
        if not client.bucket_exists(silver_bucket):
            raise ValueError(f"Silver bucket '{silver_bucket}' does not exist")
            
        # Create gold bucket if it doesn't exist
        if not client.bucket_exists(gold_bucket):
            logging.info(f"Creating gold bucket: {gold_bucket}")
            client.make_bucket(gold_bucket)
        
        # Read all silver layer parquet files into Spark DataFrame
        objects = list(client.list_objects(silver_bucket, prefix="breweries_by_location/", recursive=True))
        
        if not objects:
            raise ValueError(f"No objects found in silver bucket with prefix 'breweries_by_location/'")
            
        logging.info(f"Found {len(objects)} objects in silver bucket")
        
        parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
        
        if not parquet_files:
            raise ValueError("No parquet files found in silver bucket")
            
        logging.info(f"Processing {len(parquet_files)} parquet files")
        
        # Download and combine parquet files
        temp_dir = get_temp_directory(context, "parquet_download")
        temp_dirs.append(temp_dir)
        
        try:
            for i, obj in enumerate(parquet_files):
                try:
                    logging.info(f"Downloading file {i+1}/{len(parquet_files)}: {obj.object_name}")
                    response = client.get_object(silver_bucket, obj.object_name)
                    temp_file = os.path.join(temp_dir, f"temp_{i}.parquet")
                    
                    with open(temp_file, 'wb') as f:
                        f.write(response.data)
                    temp_files.append(temp_file)
                    
                    # Log file size for monitoring
                    file_size_mb = len(response.data) / (1024 * 1024)
                    logging.info(f"Downloaded {obj.object_name}: {file_size_mb:.2f} MB")
                    
                except Exception as e:
                    logging.error(f"Error downloading parquet file {obj.object_name}: {e}")
                    raise RuntimeError(f"Failed to download parquet file {obj.object_name}: {e}")
                    
        except Exception as e:
            cleanup_temp_files(temp_files, temp_dirs)
            raise e
        
        if not temp_files:
            raise ValueError("No valid data found in silver layer parquet files")
        
        # Read combined data into Spark DataFrame
        try:
            combined_df = spark.read.parquet(*temp_files)
            
            # Validate data quality
            quality_metrics = validate_data_quality(
                combined_df, context, 
                min_records=int(os.getenv("MIN_RECORDS_GOLD", "1"))
            )
            record_count = quality_metrics['record_count']
            logging.info(f"Loaded {record_count} total records from {len(temp_files)} silver layer files")
            
        finally:
            # Clean up temp files
            cleanup_temp_files(temp_files, temp_dirs)
        
        # Validate required columns
        required_columns = ['id', 'brewery_type', 'country', 'state', 'city', 'has_coordinates', 'has_website', 'has_phone']
        actual_columns = combined_df.columns
        missing_columns = [col_name for col_name in required_columns if col_name not in actual_columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in silver data: {missing_columns}")
        
        # Create analytical aggregations
        
        # 1. Breweries count by type and location
        brewery_by_type_location = combined_df.groupBy('brewery_type', 'country', 'state', 'city').agg(
            count('id').alias('brewery_count'),
            spark_sum('has_coordinates').alias('has_coordinates'),
            spark_sum('has_website').alias('has_website'),
            spark_sum('has_phone').alias('has_phone')
        )
        
        # Add percentage calculations (safe division)
        brewery_by_type_location = brewery_by_type_location.withColumn(
            'pct_with_coordinates',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_coordinates') / col('brewery_count') * 100)).otherwise(0), 2)
        ).withColumn(
            'pct_with_website',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_website') / col('brewery_count') * 100)).otherwise(0), 2)
        ).withColumn(
            'pct_with_phone',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_phone') / col('brewery_count') * 100)).otherwise(0), 2)
        )
        
        # 2. Summary by brewery type
        brewery_by_type = combined_df.groupBy('brewery_type').agg(
            count('id').alias('brewery_count'),
            spark_sum('has_coordinates').alias('has_coordinates'),
            spark_sum('has_website').alias('has_website'),
            spark_sum('has_phone').alias('has_phone')
        )
        
        brewery_by_type = brewery_by_type.withColumn(
            'pct_with_coordinates',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_coordinates') / col('brewery_count') * 100)).otherwise(0), 2)
        ).withColumn(
            'pct_with_website',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_website') / col('brewery_count') * 100)).otherwise(0), 2)
        ).withColumn(
            'pct_with_phone',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_phone') / col('brewery_count') * 100)).otherwise(0), 2)
        )
        
        # 3. Summary by location (country/state/city)
        from pyspark.sql.functions import countDistinct
        brewery_by_location = combined_df.groupBy('country', 'state', 'city').agg(
            count('id').alias('brewery_count'),
            countDistinct('brewery_type').alias('unique_brewery_types'),
            spark_sum('has_coordinates').alias('has_coordinates'),
            spark_sum('has_website').alias('has_website'),
            spark_sum('has_phone').alias('has_phone')
        )
        
        brewery_by_location = brewery_by_location.withColumn(
            'pct_with_coordinates',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_coordinates') / col('brewery_count') * 100)).otherwise(0), 2)
        )
        
        # 4. Overall summary statistics
        total_breweries = combined_df.count()
        unique_countries = combined_df.select('country').distinct().count()
        unique_states = combined_df.select('state').distinct().count()
        unique_cities = combined_df.select('city').distinct().count()
        unique_brewery_types = combined_df.select('brewery_type').distinct().count()
        breweries_with_coordinates = combined_df.agg(spark_sum('has_coordinates')).collect()[0][0]
        breweries_with_website = combined_df.agg(spark_sum('has_website')).collect()[0][0]
        breweries_with_phone = combined_df.agg(spark_sum('has_phone')).collect()[0][0]
        
        summary_stats = {
            'total_breweries': total_breweries,
            'unique_countries': unique_countries,
            'unique_states': unique_states,
            'unique_cities': unique_cities,
            'unique_brewery_types': unique_brewery_types,
            'breweries_with_coordinates': int(breweries_with_coordinates) if breweries_with_coordinates else 0,
            'breweries_with_website': int(breweries_with_website) if breweries_with_website else 0,
            'breweries_with_phone': int(breweries_with_phone) if breweries_with_phone else 0,
            'processed_at': datetime.now().isoformat()
        }
        
        # Add percentages to summary
        if total_breweries > 0:
            summary_stats['pct_with_coordinates'] = round(
                summary_stats['breweries_with_coordinates'] / summary_stats['total_breweries'] * 100, 2
            )
            summary_stats['pct_with_website'] = round(
                summary_stats['breweries_with_website'] / summary_stats['total_breweries'] * 100, 2
            )
            summary_stats['pct_with_phone'] = round(
                summary_stats['breweries_with_phone'] / summary_stats['total_breweries'] * 100, 2
            )
        else:
            summary_stats['pct_with_coordinates'] = 0.0
            summary_stats['pct_with_website'] = 0.0
            summary_stats['pct_with_phone'] = 0.0
        
        # Generate timestamp for file naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save aggregations as both parquet and JSON for different use cases
        aggregations = {
            'brewery_by_type_location': brewery_by_type_location,
            'brewery_by_type': brewery_by_type,
            'brewery_by_location': brewery_by_location
        }
        
        files_created = []
        
        for agg_name, agg_df in aggregations.items():
            record_count = agg_df.count()
            logging.info(f"Saving aggregation: {agg_name} ({record_count} records)")
            
            try:
                # Save as parquet for analytical queries
                temp_parquet = f"/tmp/{agg_name}_{timestamp}.parquet"
                agg_df.coalesce(1).write.mode('overwrite').parquet(temp_parquet)
                
                # Read the single parquet file and upload to MinIO
                import glob
                parquet_files = glob.glob(f"{temp_parquet}/part-*.parquet")
                if parquet_files:
                    with open(parquet_files[0], 'rb') as f:
                        parquet_data = f.read()
                    
                    parquet_filename = f"analytics/{agg_name}/{agg_name}_{timestamp}.parquet"
                    parquet_buffer = io.BytesIO(parquet_data)
                    client.put_object(
                        gold_bucket,
                        parquet_filename,
                        parquet_buffer,
                        length=len(parquet_data),
                        content_type="application/octet-stream"
                    )
                    files_created.append(parquet_filename)
                    
                    # Clean up temp parquet directory
                    import shutil
                    shutil.rmtree(temp_parquet, ignore_errors=True)
                
            except Exception as e:
                logging.error(f"Error saving parquet for {agg_name}: {e}")
                raise RuntimeError(f"Failed to save parquet for {agg_name}: {e}")
            
            # Save as JSON for API consumption
            json_data = json.dumps(agg_df.toPandas().to_dict('records'), indent=2)
            json_buffer = io.BytesIO(json_data.encode('utf-8'))
            
            json_filename = f"analytics/{agg_name}/{agg_name}_{timestamp}.json"
            client.put_object(
                gold_bucket,
                json_filename,
                json_buffer,
                length=len(json_data.encode('utf-8')),
                content_type="application/json"
            )
            files_created.append(json_filename)
        
        # Save summary statistics
        summary_json = json.dumps(summary_stats, indent=2)
        summary_buffer = io.BytesIO(summary_json.encode('utf-8'))
        
        summary_filename = f"analytics/summary/brewery_summary_{timestamp}.json"
        client.put_object(
            gold_bucket,
            summary_filename,
            summary_buffer,
            length=len(summary_json.encode('utf-8')),
            content_type="application/json"
        )
        files_created.append(summary_filename)
        
        # Store results in XCom
        context['ti'].xcom_push(key='files_created', value=files_created)
        context['ti'].xcom_push(key='summary_stats', value=summary_stats)
        
        logging.info(f"Gold layer aggregation completed. Created {len(files_created)} files")
        logging.info(f"Summary: {summary_stats['total_breweries']} breweries across {summary_stats['unique_countries']} countries")
        
        return files_created
        
    except S3Error as e:
        logging.error(f"MinIO S3 error during gold aggregation: {e}")
        raise RuntimeError(f"MinIO S3 error during gold aggregation: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during gold aggregation: {e}")
        raise RuntimeError(f"Unexpected error during gold aggregation: {e}")
        finally:
            # Final cleanup
            cleanup_temp_files(temp_files, temp_dirs)

with DAG(
    'medallion_gold_layer',
    default_args=default_args,
    description='Gold Layer: Create analytical aggregations of brewery data by type and location',
    schedule=None,  # Triggered by silver layer completion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'gold', 'analytics', 'aggregation'],
) as dag:

    create_aggregations = PythonOperator(
        task_id='aggregate_brewery_data',
        python_callable=aggregate_brewery_data,
    )