
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
