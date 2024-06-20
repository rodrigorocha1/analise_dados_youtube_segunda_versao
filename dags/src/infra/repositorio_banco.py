try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Dict
from pandas import DataFrame
import pandas as pd
from src.infra.interface.repositorio_banco_interface import RepositorioBancoInterface
from src.infra.conexao_banco import ConexaoBanco
from sqlalchemy import create_engine


class RepositorioBanco(RepositorioBancoInterface):

    @classmethod
    def consultar_banco(cls, consulta_sql: str, tipos_dados: Dict) -> DataFrame:
        """Método para conectar no banco

        Args:
            consulta_sql (str): uma consulta sql
            tipos_dados (Dict): tipos de dados

        Returns:
            DataFrame: Dataframe da consulta sql
        """
    
        conn = ConexaoBanco.conexao
        ConexaoBanco.connect()

        engine = create_engine('hive://', creator=lambda: conn)

        df_resultado = pd.read_sql_query(
                consulta_sql,
                dtype=tipos_dados,
                con=engine
        )
        return df_resultado
       
