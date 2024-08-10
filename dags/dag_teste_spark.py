from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import DAG
import pendulum

with DAG(
    dag_id='teste_dag_spark',
    schedule_interval=None,
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
) as dag:
    transformacao_dados_canais = SparkSubmitOperator(
        task_id='spark_transformacao_dados_canais',
        conn_id='spark',
        application="/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/spark_etl/transform.py",
        application_args=['--opcao', 'C', '--caminho_arquivo',
                          str(f'/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/bronze/*/*/estatisticas_canais_brasileiros/req_estatisticas_canais_brasileiros.json')],

    )

    transformacao_dados_videos = SparkSubmitOperator(
        task_id='spark_transformacao_dados_videos',
        conn_id='spark',
        application="/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/spark_etl/transform.py",
        application_args=['--opcao', 'V', '--caminho_arquivo',
                          str(f'/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/bronze/*/*/estatisticas_videos/req_estatisticas_videos.json')],

    )
    transformacao_dados_canais >> transformacao_dados_videos
