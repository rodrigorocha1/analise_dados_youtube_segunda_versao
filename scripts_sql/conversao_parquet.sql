SET min_salary = 30000;
COPY (SELECT *
from read_parquet(
'/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/prata/cities_skylines/extracao_data_2024_06_29/estatisticas_canais_brasileiros/req_estatisticas_canais_brasileiros.parquet')
)
TO '/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_canais_brasileiros.parquet' (FORMAT PARQUET);