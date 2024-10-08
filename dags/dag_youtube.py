try:
    import sys
    import os
    sys.path.append(os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')))
except:
    pass
import pendulum
from unidecode import unidecode
from airflow.operators.empty import EmptyOperator
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from src.dados.infra_json import InfraJson
from src.dados.infra_pickle import InfraPicke
from variaveis.variaveis import lista_assunto
from operators.youtube_busca_operator import YoutubeBuscaOperator
from operators.youtube_busca_canais_operator import YoutubeBuscaCanaisOperator
from operators.youtube_busca_videos_operator import YoutubeBuscaVideoOperator
from hook.youtube_busca_video_hook import YoutubeBuscaVideoHook
from hook.youtube_busca_canais_hook import YoutubeBuscaCanaisHook
from hook.youtube_busca_pesquisa_hook import YoutubeBuscaPesquisaHook


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
    dag_id='extracao_youtube',
    schedule_interval='0 11,17,22 * * *',
    # schedule_interval=None,
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
) as dag:

    task_inicio = EmptyOperator(
        task_id='task_inicio_dag',
        dag=dag
    )
    with TaskGroup('task_youtube_api_historico_pesquisa', dag=dag) as tg1:
        lista_task_historico = []
        for termo_assunto in lista_assunto:
            id_termo_assunto = unidecode(
                termo_assunto.lower().replace(' ', '_'))
            id_task = f'id_youtube_api_historico_pesquisa_{id_termo_assunto}'
            extracao_api_youtube_historico_pesquisa = YoutubeBuscaOperator(
                task_id=id_task,
                assunto=id_termo_assunto,
                ordem_extracao=YoutubeBuscaPesquisaHook(
                    consulta=termo_assunto,
                    data_inicio=data_hora_busca
                ),
                extracao_manipulacao_dados=(
                    InfraJson(
                        camada_datalake='bronze',
                        assunto=id_termo_assunto,
                        pasta=data,
                        metrica='historico_pesquisa',
                        nome_arquivo='historico_pesquisa.json',
                    ),
                    InfraPicke(
                        camada_datalake='bronze',
                        assunto=id_termo_assunto,
                        pasta=None,
                        metrica=None,
                        nome_arquivo='id_videos.pkl'
                    ),
                    InfraPicke(
                        camada_datalake='bronze',
                        assunto=id_termo_assunto,
                        pasta=None,
                        metrica=None,
                        nome_arquivo='id_canais.pkl'
                    )
                ),
            )
        lista_task_historico.append(
            extracao_api_youtube_historico_pesquisa
        )

    with TaskGroup('task_youtube_dados_canais', dag=dag) as tg2:
        lista_task_canais = []
        for termo_assunto in lista_assunto:
            id_termo_assunto = unidecode(
                termo_assunto.lower().replace(' ', '_'))
            id_task = f'id_youtube_api_dados_canais_{id_termo_assunto}'
            extracao_youtube_canais = YoutubeBuscaCanaisOperator(
                assunto=id_termo_assunto,
                task_id=id_task,
                extracao_manipulacao_dados=None,
                extracao_unica=InfraJson(
                    camada_datalake='bronze',
                    assunto=id_termo_assunto,
                    pasta=data,
                    metrica='estatisticas_canais_brasileiros',
                    nome_arquivo='req_estatisticas_canais_brasileiros.json'

                ),
                ordem_extracao=YoutubeBuscaCanaisHook(
                    carregar_dados=InfraPicke(
                        camada_datalake='bronze',
                        assunto=id_termo_assunto,
                        pasta=None,
                        metrica=None,
                        nome_arquivo='id_canais.pkl'
                    ),
                )
            )

            lista_task_canais.append(extracao_youtube_canais)

    with TaskGroup('task_youtube_dados_video', dag=dag) as tg3:
        lista_task_canais = []
        for termo_assunto in lista_assunto:
            id_termo_assunto = unidecode(
                termo_assunto.lower().replace(' ', '_'))
            id_task = f'id_youtube_api_dados_video_{id_termo_assunto}'
            extracao_dados_video = YoutubeBuscaVideoOperator(
                task_id=id_task,
                assunto=id_termo_assunto,
                extracao_manipulacao_dados=(InfraPicke(
                    camada_datalake='bronze',
                    assunto=id_termo_assunto,
                    pasta=None,
                    metrica=None,
                    nome_arquivo='id_videos_comentarios.pkl'
                )),
                ordem_extracao=YoutubeBuscaVideoHook(
                    carregar_dados=InfraPicke(
                        camada_datalake='bronze',
                        assunto=id_termo_assunto,
                        pasta=None,
                        metrica=None,
                        nome_arquivo='id_videos.pkl'
                    )
                ),
                extracao_unica=InfraJson(
                    camada_datalake='bronze',
                    assunto=id_termo_assunto,
                    pasta=data,
                    metrica='estatisticas_videos',
                    nome_arquivo='req_estatisticas_videos.json'

                )
            )
            lista_task_canais.append(extracao_dados_video)

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


task_inicio >> tg1 >> tg2 >> tg3 >> transformacao_dados_canais >> transformacao_dados_videos >> task_fim

# task_inicio >> transformacao_dados_canais >> transformacao_dados_videos >> task_fim


# task_inicio >> tg2 >> task_fim
# task_inicio >> transform_spark_submit >> task_fim
