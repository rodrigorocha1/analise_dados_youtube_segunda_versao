try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Dict, Tuple
from abc import ABC, abstractmethod
from airflow.models import BaseOperator
from hook.youtube_hook import YoutubeHook
from src.dados.dados_youtube import DadosYoutube
from src.dados.iinfra_dados import IInfraDados


class YoutubeOperator(BaseOperator, ABC):
    template_fields = ['ordem_extracao']

    def __init__(
        self,
            ordem_extracao: YoutubeHook,
        extracao_dados: Tuple[IInfraDados],
            **kwargs,
    ):
        """init para youtube operator

        Args:
            ordem_extracao (str): ordem de extracao, recebe um Hook
        """
        self._extracao_dados = extracao_dados
        self.ordem_extracao = ordem_extracao
        self.dados_youtube = DadosYoutube()
        super().__init__(**kwargs)

    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
