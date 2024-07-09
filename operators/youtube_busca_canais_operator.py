from typing import Dict, Tuple
from hook.youtube_hook import YoutubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.iinfra_dados import IInfraDados


class YoutubeBuscaCanaisOperator(YoutubeOperator):
    template_fields = [
        'ordem_extracao'
    ]

    def __init__(self, ordem_extracao: YoutubeHook, extracao_manipulacao_dados: Tuple[IInfraDados], assunto: str, extracao_unica: IInfraDados = None, **kwargs):
        super().__init__(ordem_extracao=ordem_extracao, extracao_manipulacao_dados=extracao_manipulacao_dados,
                         assunto=assunto, extracao_unica=extracao_unica, **kwargs)

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            req['assunto'] = self._assunto
            self._extracao_unica.salvar_dados(req=req)

    def execute(self, context):
        try:
            for json_response in self.ordem_extracao.run():
                self.gravar_dados(json_response)
        except:
            exit
