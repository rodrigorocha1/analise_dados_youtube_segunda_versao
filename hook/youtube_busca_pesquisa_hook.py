from hook.youtube_hook import YoutubeHook
from src.dados.iinfra_dados import IInfraDados
import variaveis.variaveis as v


class YoutubeBuscaPesquisaHook(YoutubeHook):
    def __init__(self, data_inicio: str, consulta: str = None, conn_id: str = None, carregar_dados: IInfraDados = None) -> None:
        """Classe (Dag) para buscar os resultados de pesquisas

        Args:
            data_inicio (str): data de inicio
            consulta (str, optional): termo de busc. Defaults to None.
            conn_id (str, optional): id da dag. Defaults to None.
            carregar_dados (IInfraDados, optional): Carregar o objeto de arquivo que bai ser shamafo. Defaults to None.
        """
        self._consulta = consulta
        self._data_inicio = data_inicio
        super().__init__(conn_id, carregar_dados)

    def _criar_url(self) -> str:
        """Retorna a url

        Returns:
            str: _description_
        """
        return self._URL + '/search/'

    def run(self):
        """Método para rodar a dag

        Returns:
            _type_: _description_
        """
        session = self.get_conn()

        url = self._criar_url()

        params = [
            {
                'part':  'snippet',
                'key': v.chave_youtube,
                'regionCode': 'BR',
                'relevanceLanguage': 'pt',
                'maxResults': '50',
                'publishedAfter': self._data_inicio,
                'q': self._consulta,
                'pageToken': ''
            }
        ]

        response = self._executar_paginacao(
            url=url, session=session, params=params)
        return response
