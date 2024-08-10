SELECT *
FROM read_parquet('/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_canais/*/*/*/*/*/*.parquet');

SELECT *
FROM read_parquet('/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_videos/*/*/*/*/*/*/*/*.parquet');


SELECT *
FROM read_parquet('/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_videos/*/*/*/*/*/*/*/*.parquet')
WHERE ASSUNTO='elder_ring'

ORDER BY DATA_EXTRACAO ASC;



# Desempenho do vídeos por dia (Likes/Comentários/Visualizações) (Faça uma variação com o dia anterior)


SELECT DIA_EXTRACAO, 
	max(TOTAL_VISUALIZACOES) AS TOTAL_VISUALIZACOES_ACC
FROM read_parquet('/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_videos/*/*/*/*/*/*/*/*.parquet')
WHERE ASSUNTO='elder_ring'
AND ID_VIDEO IN ('wlgI9eu4XoE')
GROUP BY DIA_EXTRACAO
ORDER BY DIA_EXTRACAO;



Análise de palavras chaves dos vídeos


SELECT TAGS
FROM read_parquet('/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_videos/*/*/*/*/*/*/*/*.parquet')
WHERE ASSUNTO='elder_ring';

# Top 10 Análise engajamento

# -  [(Número de Curtidas + Número de Comentários + Número de Compartilhamentos) / Número Total de Visualizações] * 100
SELECT ID_VIDEO , (TOTAL_LIKES + )
FROM read_parquet('/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_videos/*/*/*/*/*/*/*/*.parquet')
WHERE ASSUNTO='elder_ring'
AND ID_VIDEO IN ('wlgI9eu4XoE')
order BY DATA_EXTRACAO;



