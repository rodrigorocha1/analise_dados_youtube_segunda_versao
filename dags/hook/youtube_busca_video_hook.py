
from hook.youtube_hook import YoutubeHook
from src.dados.iinfra_dados import IInfraDados
import variaveis.variaveis as v


class YoutubeBuscaVideoHook(YoutubeHook):
    def __init__(self, data_inicio: str, consulta: str = None, conn_id: str = None, carregar_dados: IInfraDados = None) -> None:
        super().__init__(data_inicio, consulta, conn_id, carregar_dados)

    def _criar_url(self):
        return self._URL + '/videos/'

    def run(self):
        session = self.get_conn()
        lista_videos = self._carregar_dados.carregar_dados()
        url = self._criar_url()
        params = [
            {
                'part':  'statistics,contentDetails,id,snippet,status',
                'id': id_video,
                'key': v.chave_youtube,
                'regionCode': 'BR',
                'pageToken': ''

            } for id_video in lista_videos
        ]
        print('consultando', self._consulta)
        print('lista videos', lista_videos)
        print('total de vídeos', len(lista_videos))
        print('params', params)

        response = self._executar_paginacao(
            url=url,
            session=session,
            params=params
        )
        return response
