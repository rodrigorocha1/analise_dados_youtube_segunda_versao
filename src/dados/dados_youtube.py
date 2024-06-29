from typing import Dict, List
import requests
import variaveis.variaveis as v
from src.dados.infra_pickle import InfraPicke


class DadosYoutube():

    @classmethod
    def verificar_idioma_canal(cls, id_canal: str) -> bool:
        """Método para verificar se o canal é brasileiro

        Args:
            id_canal (str): id do canal

        Returns:
            bool: verdadeiro ou falso
        """
        try:
            params = {
                'part': 'snippet,contentDetails, id',
                'key': v.chave_youtube,
                'id': id_canal,
                'maxResults': '100'
            }
            url = v.url_youtube + '/channels/'
            response = requests.get(url=url, params=params)
            req = response.json()
            flag = req['items'][0]['snippet']['country']
            if flag == 'BR':
                return True
            return False
        except:
            return False

    @classmethod
    def obter_lista_videos(cls, req: Dict) -> List[str]:
        """Método para obter os vídeos dos canais brasileiros

        Args:
            req (Dict): requisição da api do youtube

        Returns:
            List[str]: Lista de vídeos Brasileiros
        """
        lista_videos = []
        for item in req['items']:
            if cls.verificar_idioma_canal(item['snippet']['channelId']):
                lista_videos.append(item['id']['videoId'])
        return list(set(lista_videos))

    @classmethod
    def obter_lista_comentarios(cls, req: Dict) -> List[str]:
        lista_id_comentarios_encandeados = []
        for comment in req['items']:
            lista_id_comentarios_encandeados.append(comment['id'])
        return lista_id_comentarios_encandeados

    @classmethod
    def obter_lista_videos_comentarios(cls, req: Dict) -> List[str]:
        lista_id_videos = []
        if int(req['items'][0]['statistics']['commentCount']) > 0:
            lista_id_videos.append(
                req['items'][0]['id'])
            return lista_id_videos

    @classmethod
    def obter_lista_canais_brasileiros(cls, req: Dict, infra: InfraPicke) -> List[str]:
        lista_id_canais = []
        # abrir lista canais salvos
        lista_canais_salvos = infra.carregar_dados()
        lista_canais_salvos = [] if lista_canais_salvos is None else lista_canais_salvos
        # fazer for da requisicao:
        for canal in req['items']:
            id_canal = canal['snippet']['channelId']
            if id_canal not in lista_canais_salvos:
                if cls.verificar_idioma_canal(id_canal):
                    lista_id_canais.append(canal['snippet']['channelId'])
        return lista_id_canais
