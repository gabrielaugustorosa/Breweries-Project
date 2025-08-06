from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'Gabriel Augusto Rosa',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False,
}

def log_pipeline_start(**context):
    """Log the start of the medallion pipeline"""
    execution_date = context['execution_date']
    dag_run_id = context['dag_run'].run_id
    logging.info(f"=== STARTING MEDALLION PIPELINE ===")
    logging.info(f"Execution Date: {execution_date}")
    logging.info(f"DAG Run ID: {dag_run_id}")
    logging.info("Pipeline stages: Bronze -> Silver -> Gold")
    logging.info("Expected flow: API Data -> Raw JSON -> Cleaned Parquet -> Analytics")
    
    # Store pipeline start time for metrics
    pipeline_start_time = context['ts']
    context['ti'].xcom_push(key='pipeline_start_time', value=pipeline_start_time)

def log_pipeline_completion(**context):
    """Log the completion of the medallion pipeline"""
    execution_date = context['execution_date']
    dag_run_id = context['dag_run'].run_id
    
    # Calculate pipeline duration
    pipeline_start_time = context['ti'].xcom_pull(key='pipeline_start_time', task_ids='pipeline_start')
    pipeline_end_time = context['ts']
    
    logging.info(f"=== MEDALLION PIPELINE COMPLETED ===")
    logging.info(f"Execution Date: {execution_date}")
    logging.info(f"DAG Run ID: {dag_run_id}")
    logging.info(f"Pipeline Duration: {pipeline_start_time} to {pipeline_end_time}")
    logging.info("All stages completed: Bronze ✓ Silver ✓ Gold ✓")
    
    # Enhanced metrics collection
    pipeline_stats = {
        'execution_date': str(execution_date),
        'dag_run_id': dag_run_id,
        'pipeline_status': 'completed',
        'stages_completed': ['bronze', 'silver', 'gold'],
        'start_time': pipeline_start_time,
        'end_time': pipeline_end_time,
        'total_stages': 3
    }
    
    context['ti'].xcom_push(key='pipeline_stats', value=pipeline_stats)
    logging.info(f"Pipeline statistics: {pipeline_stats}")
    return pipeline_stats

with DAG(
    'medallion_architecture_pipeline',
    default_args=default_args,
    description='Main orchestrator for the Medallion Architecture: Bronze -> Silver -> Gold layers',
    schedule='@daily',  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'orchestrator', 'pipeline', 'brewery'],
) as dag:

    # Start of pipeline
    pipeline_start = PythonOperator(
        task_id='pipeline_start',
        python_callable=log_pipeline_start,
    )

    # Trigger Bronze Layer (Data Ingestion)
    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze_layer',
        trigger_dag_id='medallion_bronze_layer',
        wait_for_completion=True,
        poke_interval=30,
        timeout=3600,  # 1 hour timeout
        allowed_states=['success'],
        failed_states=['failed'],
        execution_timeout=timedelta(hours=1),
    )

    # Trigger Silver Layer (Data Transformation)
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_layer',
        trigger_dag_id='medallion_silver_layer',
        wait_for_completion=True,
        poke_interval=30,
        timeout=1800,  # 30 minutes timeout
        allowed_states=['success'],
        failed_states=['failed'],
        execution_timeout=timedelta(minutes=45),
    )

    # Trigger Gold Layer (Analytics)
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_layer',
        trigger_dag_id='medallion_gold_layer',
        wait_for_completion=True,
        poke_interval=30,
        timeout=1200,  # 20 minutes timeout
        allowed_states=['success'],
        failed_states=['failed'],
        execution_timeout=timedelta(minutes=30),
    )

    # Pipeline completion
    pipeline_complete = PythonOperator(
        task_id='pipeline_completion',
        python_callable=log_pipeline_completion,
    )

    # Define task dependencies
    pipeline_start >> trigger_bronze >> trigger_silver >> trigger_gold >> pipeline_complete