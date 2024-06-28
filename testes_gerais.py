import os

# Caminho da nova pasta
caminho_da_pasta = '/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/bronze'

# Cria a pasta
os.makedirs(caminho_da_pasta, exist_ok=True)

print(f'A pasta foi criada em: {caminho_da_pasta}')
