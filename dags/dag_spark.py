try:
    import sys
    import os
    sys.path.append(os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')))
except:
    pass
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def obter_turno_pasta(hora: int) -> str:
    print(hora, type(hora))
    if 0 <= hora < 6:
        turno = 'madrugada'
    elif 6 <= hora < 12:
        turno = 'manha'
    elif 12 <= hora < 18:
        turno = 'tarde'
    else:
        turno = 'noite'
    return turno


data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
data_hora_busca = data_hora_atual.subtract(hours=7)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
data = f'extracao_data_{data}_{obter_turno_pasta(hora_atual)}'

# Adicionar turno extração Turno Extração
with DAG(
    dag_id='extracao_youtube_spark',
    schedule_interval=None,
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
) as dag:

    task_inicio = EmptyOperator(
        task_id='task_inicio_dag',
        dag=dag
    )

    task_fim = EmptyOperator(
        task_id='task_fim_dag',
        dag=dag
    )

    transformacao_dados_canais = SparkSubmitOperator(
        task_id='spark_transformacao_dados_canais',
        conn_id='spark',
        application="/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/spark_etl/transform.py",
        application_args=['--opcao', 'C', '--caminho_arquivo',
                          str(f'/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/bronze/*/{data}/estatisticas_canais_brasileiros/req_estatisticas_canais_brasileiros.json')],

    )
    for i in range(1, 2):
        transformacao_dados_videos = SparkSubmitOperator(
            task_id='spark_transformacao_dados_videos',
            conn_id='spark',
            application="/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/spark_etl/transform.py",
            application_args=['--opcao', 'V', '--caminho_arquivo',
                              str(f'/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/bronze/*/{data}/estatisticas_videos/req_estatisticas_videos.json')],

        )


task_inicio >> transformacao_dados_canais >> transformacao_dados_videos >> task_fim

# task_inicio >> transformacao_dados_canais >> transformacao_dados_videos >> task_fim


# task_inicio >> tg2 >> task_fim
# task_inicio >> transform_spark_submit >> task_fim
