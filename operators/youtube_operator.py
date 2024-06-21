try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Tuple, Dict
from abc import ABC, abstractmethod
from airflow.models import BaseOperator
from hook.youtube_hook import YoutubeHook
from src.dados.iinfra_dados import IInfraDados
from src.dados.dados_youtube import DadosYoutube


class YoutubeOperator(BaseOperator, ABC):
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
            **kwargs,
    ):
        """init para youtube operator

        Args:
            ordem_extracao (str): ordem de extracao, recebe um rook
            extracao_dados (Tuple[IInfraDados]): tipo de carregamento de dados

            termo_consulta (str, optional): termo de busca . Defaults to None.
            data_inicio (str, optional): data de ínicio da pesquisa. Defaults to None.
        """
        self.ordem_extracao = ordem_extracao

        self.data_inicio = data_inicio
        self.extracao_unica = extracao_unica
        self.termo_consulta = termo_consulta
        self.extracao_dados = extracao_dados
        self.dados_youtube = DadosYoutube()
        super().__init__(**kwargs)

    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
