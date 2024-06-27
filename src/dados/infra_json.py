
import json
import os
from src.dados.infra_dados import InfraDados


class InfraJson(InfraDados):
    def __init__(self, camada_datalake: str, assunto: str, pasta: str, metrica: str, nome_arquivo: str) -> None:
        super().__init__(camada_datalake, assunto, pasta, metrica, nome_arquivo)

    def salvar_dados(self, **kwargs):
        """Método para guardar json
        """

        if not os.path.exists(self._diretorio_completo):
            os.makedirs(self._diretorio_completo, exist_ok=True)

        with open(os.path.join(self._diretorio_completo, self._nome_arquivo), 'a') as arquivo_json:
            json.dump(kwargs['req'],  arquivo_json, ensure_ascii=False)
            arquivo_json.write('\n')

    def carregar_dados(self):
        pass
