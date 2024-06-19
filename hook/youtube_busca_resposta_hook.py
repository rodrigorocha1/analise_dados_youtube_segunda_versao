from hook.youtube_hook import YoutubeHook
from src.dados.iinfra_dados import IInfraDados
import variaveis.variaveis as v


class YoutubeBuscaRespostaHook(YoutubeHook):
    def __init__(self, data_inicio: str, consulta: str = None, conn_id: str = None, carregar_dados: IInfraDados = None) -> None:
        super().__init__(data_inicio, consulta, conn_id, carregar_dados)

    def _criar_url(self):
        return self._URL + '/comments/'

    def run(self):
        session = self.get_conn()
        lista_comentarios = self._carregar_dados.carregar_dados()
        url = self._criar_url()
        print('consultando', self._consulta)
        print('total de Comentários', len(lista_comentarios))
        params = [
            {
                'part':  'snippet',
                'parentId': id_comentario,
                'key': v.chave_youtube,
                'textFormat': 'plainText',
                'pageToken': ''

            } for id_comentario in lista_comentarios
        ]
        print(params)

        response = self._executar_paginacao(
            url=url, session=session, params=params)
        return response
