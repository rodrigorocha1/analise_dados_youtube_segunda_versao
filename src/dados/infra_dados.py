import os
from src.dados.iinfra_dados import IInfraDados


class InfraDados(IInfraDados):
    def __init__(
        self,
            camada_datalake: str,
            assunto: str,
            pasta: str,
            metrica: str,
            nome_arquivo: str,

    ) -> None:
        """_summary_

        Args:
            camada_datalake (str): camada do datalake
            assunto (str): assunto da pesquisa
            pasta (str): pasta para salvar dados
            metrica (str): metrica - Pode ser none
            nome_arquivo (str): nome do arquivo
        """
        self.__caminho_base = os.getcwd()
        self.__camada_datalake = camada_datalake
        self.__assunto = assunto
        self.__pasta = pasta
        self.__metrica = metrica
        self._nome_arquivo = nome_arquivo
        if self.__metrica is not None:
            self._diretorio_completo = os.path.join(
                self.__caminho_base,
                'data',
                self.__camada_datalake,
                self.__assunto,
                self.__pasta,
                self.__metrica,
                self._nome_arquivo

            )
        else:
            self._caminho_completo = os.path.join(
                self.__caminho_base,
                'data',
                self.__camada_datalake,
                self.__assunto,
                self._nome_arquivo

            )
