
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

    def __init__(self, ordem_extracao: YoutubeHook, extracao_dados: Tuple[IInfraDados], extracao_unica: IInfraDados = None, termo_pesquisa: str = None, data_inicio: str = None, **kwargs):
        """init para youtube Busca

        Args:
            ordem_extracao (str): ordem de extracao, recebe um Hook
            extracao_dados (Tuple[IInfraDados]): tipo de carregamento de dados

            termo_pesquisa (str, optional): termo de busca . Defaults to None.
            data_inicio (str, optional): data de ínicio da pesquisa. Defaults to None.
        """
        super().__init__(ordem_extracao, extracao_dados,
                         extracao_unica, termo_pesquisa, data_inicio, **kwargs)

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
