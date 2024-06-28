from typing import Dict, List, Iterable
from datetime import datetime
from abc import ABC, abstractmethod
import requests
from airflow.providers.http.hooks.http import HttpHook
from src.dados.iinfra_dados import IInfraDados
import variaveis.variaveis as v


class YoutubeHook(HttpHook, ABC):
    def __init__(self,  data_inicio: str, consulta: str = None, conn_id: str = None, carregar_dados: IInfraDados = None) -> None:
        """Método para inicializar o youtube hook

        Args:
            data_inicio (str): data e hora de inicio da pesquisa
            consulta (str, optional): termo para consulta na api do youtube. Defaults to None.
            conn_id (str, optional): id do airflow. Defaults to None.
            carregar_dados (IInfraDados, optional): tipo de carregamento de dados. Defaults to None.
        """
        self._conn_id = conn_id
        self._data_inicio = data_inicio

        self._CHAVE = v.chave_youtube
        self._URL = v.url_youtube
        self._carregar_dados = carregar_dados
        super().__init__(http_conn_id=self._conn_id)

    @abstractmethod
    def _criar_url(self):
        pass

    def _executar_paginacao(self, url: str, session, params: List[Dict]) -> Iterable[Dict]:
        """Gerador para obter requisição da api do youtube

        Args:
            url (str): url da api
            session (_type_): session airflow
            params (List[Dict]): lista de parâmetos requeridos pela api

        Returns:
            Iterable[Dict]: a requisição da api

        Yields:
            Iterator[Iterable[Dict]]: um json com as requisições da api
        """
        i = 1
        next_token = ''
        for param in params:
            while next_token is not None:
                response = self.conectar_api(url, param, session)
                if response:
                    json_response = response.json()
                    json_response['data_extracao'] = datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'
                    )

                    yield json_response
                    try:
                        next_token = json_response['nextPageToken']
                        param['pageToken'] = next_token
                        print('próximo token', next_token)
                    except KeyError:
                        break

                else:
                    break

    def conectar_api(self, url: str, params: Dict, session) -> requests.models.Response | bool:
        """Método para conectar na api

        Args:
            url (str): url escolhida
            params (Dict): params da api
            session (_type_): session do airflow

        Returns:
            _type_: a conexão
        """
        try:
            response = requests.Request('GET', url=url, params=params)

            prep = session.prepare_request(response)

            return self.run_and_check(session, prep, {})
        except:
            return False

    @abstractmethod
    def run(self):
        pass
