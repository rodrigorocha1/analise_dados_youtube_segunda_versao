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

from src.dados.infra_json import InfraJson
from src.dados.infra_pickle import InfraPicke
from operators.youtube_busca_operator import YoutubeBuscaOperator
from operators.youtube_busca_videos_operator import YoutubeBuscaVideoOperator
from operators.youtube_busca_respostas_operator import YoutubeBuscaRespostasOperator
from operators.youtube_busca_comentarios_operator import YoutubeBuscaComentariosOperator
from operators.youtube_busca_trends_operator import YoutubeBuscaTrendsOperator
from hook.youtube_trends_hook import YoutubeTrendsYook
from hook.youtube_busca_pesquisa_hook import YoutubeBuscaPesquisaHook
from hook.youtube_busca_video_hook import YoutubeBuscaVideoHook
from hook.youtube_busca_comentario_hook import YoutubeBuscaComentarioHook
from hook.youtube_busca_resposta_hook import YoutubeBuscaRespostaHook
import logging


data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca = data_hora_atual.subtract(hours=7)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')


lista_assunto = [
    'Power BI',
    'Python AND dados',
    'Cities Skylines',
    'Cities Skylines 2',
    'Linux',
    'Linux Gamming',
    'genshin impact',
    'zelda'
]


data = 'extracao_data_' + data_hora_busca.split('T')[0].replace('-', '_')


with DAG(
    dag_id='extracao_youtube',
    schedule_interval=None,
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
                data_inicio=data_hora_busca,
                ordem_extracao=YoutubeBuscaPesquisaHook(
                    consulta=termo_assunto,
                    data_inicio=data_hora_busca
                ),
                extracao_dados=(
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
                termo_pesquisa=termo_assunto

            )

        lista_task_historico.append(
            extracao_api_youtube_historico_pesquisa
        )

# with TaskGroup('tsk_extracao_api_youtube_dados_videos_estatistica', dag=dag) as tg2:
#     lista_task_dados_videos = []
#     for termo_assunto in lista_assunto:
#         id_termo_assunto = termo_assunto.replace(
#             ' ', '_').lower().replace('|', '_')
#         termo_assunto_pasta = termo_assunto.replace(
#             ' ', '_').replace('|', '_').replace('ã', 'a').replace('ç', 'c')
#         extracao_api_youtube_dados_videos_estatistica = YoutubeBuscaVideoOperator(
#             task_id=f'id_extracao_api_youtube_dados_videos_estatistica_{
#                 id_termo_assunto}',
#             data_inicio=None,
#             ordem_extracao=YoutubeBuscaVideoHook(
#                 data_inicio=data_hora_busca,
#                 carregar_dados=InfraPicke(
#                     diretorio_datalake='bronze',
#                     termo_assunto=f'assunto_{termo_assunto_pasta}',
#                     path_extracao='id_video',
#                     metrica=None,
#                     nome_arquivo='id_video.pkl'
#                 )
#             ),
#             extracao_dados=None,
#             extracao_unica=InfraJson(
#                 diretorio_datalake='bronze',
#                 termo_assunto=f'assunto_{termo_assunto_pasta}',
#                 path_extracao=data,
#                 metrica='estatisticas',
#                 nome_arquivo='req_video.json'

#             )
#         )
#         lista_task_dados_videos.append(
#             extracao_api_youtube_dados_videos_estatistica
#         )

# extracao_api_video_trends = YoutubeBuscaTrendsOperator(
#     task_id='id_extracao_api_video_trends',
#     data_inicio=data_hora_busca,
#     ordem_extracao=YoutubeTrendsYook(
#         data_inicio=data_hora_busca
#     ),
#     extracao_dados=None,
#     extracao_unica=InfraJson(
#         diretorio_datalake='bronze',
#         termo_assunto='top_brazil',
#         path_extracao=data,
#         metrica='top_brazil',
#         nome_arquivo='req_top_brazil.json'
#     )
# )

    task_fim = EmptyOperator(
        task_id='task_fim_dag',
        dag=dag
    )


task_inicio >> tg1 >> task_fim


# task_inicio >> transform_spark_submit >> task_fim
