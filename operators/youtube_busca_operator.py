
from typing import Dict, Tuple
from hook.youtube_hook import YoutubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.iinfra_dados import IInfraDados


class YoutubeBuscaOperator(YoutubeOperator):
    template_fields = [
        'ordem_extracao',
        'extracao_dados',
        'extracao_unica',
        'termo_consulta',
        'data_inicio'
    ]

    def __init__(
        self,
            ordem_extracao: YoutubeHook,
            extracao_dados: Tuple[IInfraDados],
            extracao_unica: IInfraDados = None,
            termo_consulta: str = None,
            data_inicio: str = None,
            **kwargs
    ):
        super().__init__(
            ordem_extracao=ordem_extracao,
            extracao_dados=extracao_dados,
            extracao_unica=extracao_unica,
            termo_consulta=termo_consulta,
            data_inicio=data_inicio,
            **kwargs
        )
        print(self.ordem_extracao)

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            self.extracao_dados[0].salvar_dados(req=req)
            lista_de_videos = self.dados_youtube.obter_lista_videos(req)
            self.extracao_dados[1].salvar_dados(lista=lista_de_videos)

    def execute(self, context):
        try:
            for json_response in self.ordem_extracao.run():
                self.gravar_dados(json_response)
        except:
            exit


if __name__ == '__main__':
    from airflow.models import BaseOperator, DAG, TaskInstance
    import pendulum
    from hook.youtube_busca_pesquisa_hook import YoutubeBuscaPesquisaHook
    from src.dados.infra_json import InfraJson
    from src.dados.infra_pickle import InfraPicke
    data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
    data_hora_atual = pendulum.parse(data_hora_atual)
    data_hora_busca = data_hora_atual.subtract(minutes=30)
    data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
    data = 'extracao_data_' + data_hora_busca.split('T')[0].replace('-', '_')

    with DAG(
        dag_id='extracao_youtube',
        schedule_interval=None,
        catchup=False,
        start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
    ) as dag:
        termo_assunto = 'Power BI DADOS'
        id_termo = termo_assunto.replace(' ', '').lower()
        extracao_api_youtube_historico_pesquisa = YoutubeBuscaOperator(
            task_id=f'id_youtube_api_historico_pesquisa',
            data_inicio=data_hora_busca,
            ordem_extracao=YoutubeBuscaPesquisaHook(
                data_inicio=data_hora_busca,
                consulta=termo_assunto.replace(' ', '').lower()
            ),
            termo_consulta=termo_assunto.replace(' ', '').lower(),
            extracao_dados=(
                InfraJson(
                    diretorio_datalake='bronze',
                    termo_assunto=termo_assunto.replace(' ', '_').lower(),
                    path_extracao=data,
                    metrica='requisicao_busca',
                    nome_arquivo='req_busca.json'
                ),
                InfraPicke(
                    diretorio_datalake='bronze',
                    termo_assunto=termo_assunto.replace(' ', '_').lower(),
                    path_extracao='id_video',
                    metrica=None,
                    nome_arquivo='id_video.pkl'
                )
            )
        )
        ti = TaskInstance(task=extracao_api_youtube_historico_pesquisa)
        extracao_api_youtube_historico_pesquisa.execute(ti.task_id)
