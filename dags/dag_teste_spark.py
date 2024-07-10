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
        application="/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/spark_etl/transform.py",
        application_args=['--opcao', '2', '--caminho_arquivo',
                          '/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/bronze/*/extracao_data_2024_07_09_tarde/estatisticas_videos/req_estatisticas_videos.json'],
        verbose=True,
    )

    teste_spark
