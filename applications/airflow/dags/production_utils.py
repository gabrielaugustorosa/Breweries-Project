"""
Production utilities for medallion architecture pipeline
Provides common functions for error handling, monitoring, and configuration
"""

import os
import logging
import time
import functools
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from contextlib import contextmanager
from pyspark.sql import SparkSession


def get_production_spark_config() -> Dict[str, str]:
    """Get production-ready Spark configuration"""
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"),
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        # Memory management
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "1g"),
        "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "2"),
        # Dynamic allocation
        "spark.dynamicAllocation.enabled": os.getenv("SPARK_DYNAMIC_ALLOCATION", "true"),
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": os.getenv("SPARK_MAX_EXECUTORS", "10"),
    }


@contextmanager
def get_spark_session(app_name: str, context: Dict[str, Any]):
    """Context manager for Spark session with proper cleanup"""
    spark = None
    try:
        config = get_production_spark_config()
        builder = SparkSession.builder.appName(f"{app_name}_{context['ts_nodash']}")
        
        for key, value in config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        # Set log level for production
        spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
        
        logging.info(f"Spark session created: {app_name}")
        logging.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        
        yield spark
        
    except Exception as e:
        logging.error(f"Error in Spark session {app_name}: {e}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logging.info(f"Spark session stopped: {app_name}")
            except Exception as e:
                logging.warning(f"Error stopping Spark session: {e}")


def validate_environment_variables(required_vars: list) -> Dict[str, str]:
    """Validate that all required environment variables are present"""
    missing_vars = []
    env_vars = {}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            env_vars[var] = value
    
    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}. "
            "Please ensure all required variables are set before running in production."
        )
    
    return env_vars


def retry_with_exponential_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_multiplier: float = 2.0,
    max_delay: float = 300.0
):
    """Decorator for retrying functions with exponential backoff"""
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        logging.error(f"Function {func.__name__} failed after {max_retries} retries")
                        raise e
                    
                    logging.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
                    delay = min(delay * backoff_multiplier, max_delay)
            
            raise last_exception
        return wrapper
    return decorator


def log_execution_metrics(func: Callable):
    """Decorator to log execution metrics for functions"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        function_name = func.__name__
        
        logging.info(f"Starting execution: {function_name}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            logging.info(
                f"Successfully completed: {function_name} "
                f"(execution time: {execution_time:.2f}s)"
            )
            
            # Log to context if available
            if args and hasattr(args[0], 'get') and 'ti' in args[0]:
                context = args[0]
                metrics = {
                    'function_name': function_name,
                    'execution_time': execution_time,
                    'status': 'success',
                    'timestamp': datetime.utcnow().isoformat()
                }
                context['ti'].xcom_push(key=f'{function_name}_metrics', value=metrics)
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            logging.error(
                f"Failed execution: {function_name} "
                f"(execution time: {execution_time:.2f}s, error: {e})"
            )
            
            # Log error metrics to context if available
            if args and hasattr(args[0], 'get') and 'ti' in args[0]:
                context = args[0]
                error_metrics = {
                    'function_name': function_name,
                    'execution_time': execution_time,
                    'status': 'failed',
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                }
                context['ti'].xcom_push(key=f'{function_name}_error_metrics', value=error_metrics)
            
            raise
    
    return wrapper


def cleanup_temp_files(temp_files: list, temp_dirs: list = None):
    """Safely clean up temporary files and directories"""
    # Clean up files
    for temp_file in temp_files:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                logging.debug(f"Cleaned up temp file: {temp_file}")
        except Exception as e:
            logging.warning(f"Failed to clean up temp file {temp_file}: {e}")
    
    # Clean up directories
    if temp_dirs:
        import shutil
        for temp_dir in temp_dirs:
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                    logging.debug(f"Cleaned up temp directory: {temp_dir}")
            except Exception as e:
                logging.warning(f"Failed to clean up temp directory {temp_dir}: {e}")


def get_temp_directory(context: Dict[str, Any], subdir: str = "") -> str:
    """Get a unique temporary directory for the current task"""
    base_temp_dir = os.getenv("AIRFLOW_TEMP_DIR", "/tmp")
    task_temp_dir = os.path.join(
        base_temp_dir,
        f"airflow_{context['ts_nodash']}_{context['task_instance'].task_id}",
        subdir
    )
    os.makedirs(task_temp_dir, exist_ok=True)
    return task_temp_dir


def validate_data_quality(df, context: Dict[str, Any], min_records: int = 1):
    """Validate basic data quality metrics"""
    record_count = df.count()
    
    if record_count < min_records:
        raise ValueError(
            f"Data quality check failed: Expected at least {min_records} records, "
            f"but got {record_count}"
        )
    
    # Log data quality metrics
    quality_metrics = {
        'record_count': record_count,
        'column_count': len(df.columns),
        'columns': df.columns,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    context['ti'].xcom_push(key='data_quality_metrics', value=quality_metrics)
    logging.info(f"Data quality check passed: {record_count} records, {len(df.columns)} columns")
    
    return quality_metrics


# Configuration constants
REQUIRED_MINIO_VARS = [
    'MINIO_ENDPOINT',
    'MINIO_ACCESS_KEY', 
    'MINIO_SECRET_KEY'
]

PRODUCTION_TIMEOUTS = {
    'api_request': 30,
    'minio_operation': 300,
    'spark_operation': 3600
}