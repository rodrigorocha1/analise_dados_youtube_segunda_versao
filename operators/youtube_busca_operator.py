
try:
    import sys
    import os
    sys.path.append(os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')))
except:
    pass
from typing import Dict, Tuple
from hook.youtube_hook import YoutubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.iinfra_dados import IInfraDados


class YoutubeBuscaOperator(YoutubeOperator):
    template_fields = [
        'ordem_extracao'
    ]

    def __init__(self, termo_pesquisa: str, ordem_extracao: YoutubeHook, extracao_manipulacao_dados: Tuple[IInfraDados], extracao_unica: IInfraDados = None, **kwargs):
        """__init__ para Instanciar a busca de assunto

        Args:
            termo_pesquisa (str): assunto de pesquisa
            ordem_extracao (YoutubeHook): Recebe um Hook
            extracao_manipulacao_dados (Tuple[IInfraDados]): Recebe uma tupla de infra estrutura Carregar // Salvar
            extracao_unica (IInfraDados, optional): Extração unica. Defaults to None.
        """
        self._termo_pesquisa = termo_pesquisa
        super().__init__(
            ordem_extracao=ordem_extracao,
            extracao_manipulacao_dados=extracao_manipulacao_dados,
            extracao_unica=extracao_unica, **kwargs
        )

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            self._extracao_manipulacao_dados[0].salvar_dados(req=req)
            lista_de_videos = self.dados_youtube.obter_lista_videos(req)
            self._extracao_manipulacao_dados[1].salvar_dados(
                lista=lista_de_videos)
            lista_canais = self.dados_youtube.obter_lista_canais_brasileiros(
                req=req,
                infra=self._extracao_manipulacao_dados[2]
            )
            print(f'lista canais {lista_canais}')
            self._extracao_manipulacao_dados[2].salvar_dados(
                lista=lista_canais)
            print('acabou')

            # Trazer ids dos canais brasileiros
            # Salvar ids canais brasileiross

    def execute(self, context):
        try:
            for json_response in self.ordem_extracao.run():
                self.gravar_dados(json_response)
        except Exception as e:
            print(e)
            exit
