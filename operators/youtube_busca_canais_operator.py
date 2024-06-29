from typing import Dict
from hook.youtube_hook import YoutubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.iinfra_dados import IInfraDados


class YoutubeBuscaCanaisOperator(YoutubeOperator):
    template_fields = [
        'ordem_extracao'
    ]

    def __init__(
            self,
            extracao_unica: IInfraDados,
            ordem_extracao: YoutubeHook,
            **kwargs):

        self.extracao_unica = extracao_unica
        super().__init__(ordem_extracao=ordem_extracao, **kwargs)

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            self.extracao_unica.salvar_dados(req=req)

    def execute(self, context):
        try:
            for json_response in self.ordem_extracao.run():
                self.gravar_dados(json_response)
        except:
            exit
