try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from abc import ABC, abstractmethod


class IIIInfraDados(ABC):

    @abstractmethod
    def salvar_dados(
        self,
        **kwargs
    ):
        pass

    @abstractmethod
    def carregar_dados(self):
        pass
