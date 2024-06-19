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
    def __init__(
            self,
            ordem_extracao: YoutubeHook,
            extracao_dados:
            Tuple[IInfraDados],
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

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            self.extracao_dados[0].salvar_dados(req=req)
            lista_comentarios = self.dados_youtube.obter_lista_comentarios(
                req=req)
            self.extracao_dados[1].salvar_dados(lista=lista_comentarios)

    def execute(self, context):
        try:
            for json_response in self.ordem_extracao.run():
                self.gravar_dados(json_response)
        except:
            exit
