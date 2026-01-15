from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'soufian-ch',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='related_items_fp_growth',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fp_growth_job = SparkSubmitOperator(
        task_id='fp_growth_job',
        application='/opt/spark/apps/fp_growth.py',
        conn_id='spark_default',  # tu peux créer la connexion Spark dans Airflow UI
        verbose=True,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.sql.catalog.lakehouse_catalog.uri': 'http://nessie:19120/api/v2'
        }
    )
