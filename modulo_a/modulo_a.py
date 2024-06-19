try:
    import sys
    import os
    sys.path.append(os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')))
except:
    pass
from modulo_b.modulob import exibir_modulo
# Adicionar o diretório pai ao caminho de pesquisa de módulos
print(sys.path)

exibir_modulo()
