from hook.youtube_hook import YoutubeHook
from src.dados.iinfra_dados import IInfraDados
import variaveis.variaveis as v


class YoutubeBuscaPesquisaHook(YoutubeHook):
    def __init__(self, data_inicio: str, consulta: str = None, conn_id: str = None, carregar_dados: IInfraDados = None) -> None:
        super().__init__(data_inicio, consulta, conn_id, carregar_dados)

    def _criar_url(self):
        return self._URL + '/search/'

    def run(self):
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
