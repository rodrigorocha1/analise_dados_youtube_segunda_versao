
try:
    import sys
    import os
    sys.path.append(os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')))
except:
    pass
from src.dados.infra_pickle import InfraPicke
from typing import Dict, Tuple
from hook.youtube_hook import YoutubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.iinfra_dados import IInfraDados


class YoutubeBuscaOperator(YoutubeOperator):
    template_fields = [
        'ordem_extracao',
        'extracao_dados',
        'extracao_unica',
        'termo_pesquisa',
        'data_inicio'
    ]

    def __init__(self, extracao_dados: Tuple[IInfraDados], data_inicio: str, ordem_extracao: YoutubeHook, termo_pesquisa: str, **kwargs):
        self.__ordem_extracao = ordem_extracao
        self._extracao_dados = extracao_dados
        self._data_inicio = data_inicio
        self._termo_pesquisa = termo_pesquisa
        super().__init__(ordem_extracao=self.__ordem_extracao, **kwargs)

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            self._extracao_dados[0].salvar_dados(req=req)
            lista_de_videos = self.dados_youtube.obter_lista_videos(req)
            self._extracao_dados[1].salvar_dados(lista=lista_de_videos)
            lista_canais = self.dados_youtube.obter_lista_canais_brasileiros(
                req=req,
                infra=self._extracao_dados[2]
            )
            self._extracao_dados[2].salvar_dados(lista=lista_canais)

            # Trazer ids dos canais brasileiros
            # Salvar ids canais brasileiross

    def execute(self, context):
        try:
            for json_response in self.ordem_extracao.run():
                self.gravar_dados(json_response)
        except:
            exit


if __name__ == "__main__":
    import pendulum
    from airflow.models import DAG, TaskInstance
    from unidecode import unidecode
    from hook.youtube_busca_pesquisa_hook import YoutubeBuscaPesquisaHook
    from src.dados.infra_json import InfraJson

    with DAG(
        dag_id='extracao_youtube',
        schedule_interval=None,
        catchup=False,
        start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
    ) as dag:
        data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
        data_hora_atual = pendulum.parse(data_hora_atual)
        data_hora_busca = data_hora_atual.subtract(hours=7)
        data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
        data = 'extracao_data_' + \
            data_hora_busca.split('T')[0].replace('-', '_')

        termo_assunto = 'Power BI'
        id_termo_assunto = unidecode(termo_assunto.lower().replace(' ', ''))
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
                    pasta=f'extracao_dia_{data}',
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

        ti = TaskInstance(task=extracao_api_youtube_historico_pesquisa)
        extracao_api_youtube_historico_pesquisa.execute(ti.task_id)
