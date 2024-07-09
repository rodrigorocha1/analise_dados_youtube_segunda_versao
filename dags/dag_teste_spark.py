from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import DAG

import pendulum

with DAG(
    dag_id='teste_dag_spark',
    schedule_interval=None,
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
) as dag:
    teste_spark = SparkSubmitOperator(
        task_id='spark',
        conn_id='spark',
        application="/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/spark_etl/transform_teste.py",

    )

    teste_spark
