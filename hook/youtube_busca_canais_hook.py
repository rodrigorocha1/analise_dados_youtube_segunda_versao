from hook.youtube_hook import YoutubeHook
from src.dados.iinfra_dados import IInfraDados
import variaveis.variaveis as v


class YoutubeBuscaCanaisHook(YoutubeHook):
    def __init__(self, data_inicio: str, consulta: str = None, conn_id: str = None, carregar_dados: IInfraDados = None) -> None:
        super().__init__(data_inicio, consulta, conn_id, carregar_dados)
