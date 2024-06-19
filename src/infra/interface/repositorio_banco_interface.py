from typing import Dict
from abc import ABC, abstractmethod
from pandas import DataFrame


class RepositorioBancoInterface(ABC):

    @abstractmethod
    def consultar_banco(self, consulta_sql: str, tipos_dados: Dict) -> DataFrame:
        """Método para conectar no banco

        Args:
            consulta_sql (str): uma consulta sql
            tipos_dados (Dict): tipos de dados

        Returns:
            DataFrame: Dataframe da consulta sql
        """
        pass
