from hook.youtube_hook import YoutubeHook
from operators.youtube_operator import YoutubeOperator


class YoutubeBuscaCanaisOperator(YoutubeOperator):
    def __init__(self, ordem_extracao: YoutubeHook, **kwargs):
        super().__init__(ordem_extracao, **kwargs)
