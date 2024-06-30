try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Dict, Tuple
from hook.youtube_hook import YoutubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.iinfra_dados import IInfraDados


class YoutubeBuscaComentariosOperator(YoutubeOperator):

    def __init__(self, ordem_extracao: YoutubeHook, extracao_manipulacao_dados: Tuple[IInfraDados], extracao_unica: IInfraDados = None, **kwargs):
        super().__init__(ordem_extracao=ordem_extracao,
                         extracao_manipulacao_dados=extracao_manipulacao_dados, extracao_unica=extracao_unica, **kwargs)

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            self._extracao_unica.salvar_dados(req=req)
            lista_resposta_comentarios = self.dados_youtube.obter_lista_resposta_comentarios(
                req=req)
            self._extracao_manipulacao_dados[0].salvar_dados(
                lista=lista_resposta_comentarios)

    def execute(self, context):
        try:
            for json_response in self.ordem_extracao.run():
                self.gravar_dados(json_response)
        except:
            exit


if __name__ == "__main__":
    import pendulum

    from unidecode import unidecode
    from airflow.utils.task_group import TaskGroup
    from airflow.models import BaseOperator, DAG, TaskInstance
    from hook.youtube_busca_video_hook import YoutubeBuscaVideoHook
    from operators.youtube_busca_comentarios_operator import YoutubeBuscaComentariosOperator
    data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
    data_hora_atual = pendulum.parse(data_hora_atual)
    data_hora_busca = data_hora_atual.subtract(hours=7)
    data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
    data = 'extracao_data_' + data_hora_busca.split('T')[0].replace('-', '_')
    with DAG(
        dag_id='extracao_youtube',
        schedule_interval=None,
        catchup=False,
        start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
    ) as dag:
        with TaskGroup('task_youtube_comentarios', dag=dag) as tg4:
            lista_task_comentarios = []
            for termo_assunto in lista_assunto:
                id_termo_assunto = unidecode(
                    termo_assunto.lower().replace(' ', '_'))
                id_task = f'id_youtube_api_comentarios_{id_termo_assunto}'
                extracao_dados_comentario = YoutubeBuscaComentariosOperator(
                    task_id=id_task,
                    ordem_extracao=YoutubeBuscaVideoHook(
                        carregar_dados=InfraPicke(
                            camada_datalake='bronze',
                            assunto=id_termo_assunto,
                            pasta=None,
                            metrica=None,
                            nome_arquivo='id_videos_comentarios.pkl'
                        )
                    ),
                    extracao_unica=InfraJson(
                        camada_datalake='bronze',
                        assunto=id_termo_assunto,
                        pasta=data,
                        metrica='estatisticas_videos',
                        nome_arquivo='req_comentarios.json'

                    ),
                    extracao_manipulacao_dados=(InfraPicke(
                        camada_datalake='bronze',
                        assunto=id_termo_assunto,
                        pasta=None,
                        metrica=None,
                        nome_arquivo='id_resposta_comentarios.pkl'
                    ), )


                )
                lista_task_comentarios.append(extracao_dados_comentario)
