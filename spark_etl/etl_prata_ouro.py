import re
from pyspark.sql.functions import to_timestamp, hour, col
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import duckdb


spark = SparkSession.builder.appName('DEPARA').getOrCreate()

df_estatisticas_canais = spark.read.parquet(
    '/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/prata/estatisticas_canais/estatisticas_canais.parquet')

df_estatisticas_canais.filter(
    (col('DIA_EXTRACAO') == 20) &
    (hour(col('DATA_EXTRACAO')) == 18)
).show()


df_estatisticas_canais = df_estatisticas_canais.withColumn(
    'TURNO_EXTRACAO', F.when((col('DIA_EXTRACAO') == 20) &
                             (hour(col('DATA_EXTRACAO')) == 18), 'Tarde')
    .otherwise(col('TURNO_EXTRACAO'))
)
df_estatisticas_canais_pandas = df_estatisticas_canais.toPandas()
df_estatisticas_canais_pandas.head()
df_estatisticas_canais_pandas.info()
df_estatisticas_canais_pandas[['ID_CANAL', 'ASSUNTO', 'TURNO_EXTRACAO', 'NM_CANAL']
                              ] = df_estatisticas_canais_pandas[['ID_CANAL', 'ASSUNTO', 'TURNO_EXTRACAO', 'NM_CANAL']].astype('string')
df_estatisticas_canais_pandas[['TOTAL_INSCRITOS', 'TOTAL_VIDEOS_PUBLICADOS', 'TOTAL_VISUALIZACOES']
                              ] = df_estatisticas_canais_pandas[['TOTAL_INSCRITOS', 'TOTAL_VIDEOS_PUBLICADOS', 'TOTAL_VISUALIZACOES']].astype('int64')
df_estatisticas_canais_pandas[['ANO_EXTRACAO', 'MES_EXTRACAO', 'DIA_EXTRACAO']
                              ] = df_estatisticas_canais_pandas[['ANO_EXTRACAO', 'MES_EXTRACAO', 'DIA_EXTRACAO']].astype('int32')
df_estatisticas_canais_pandas['DATA_EXTRACAO'] = df_estatisticas_canais_pandas['DATA_EXTRACAO'].astype(
    'datetime64[ns]')
df_estatisticas_canais_pandas.info()
con = duckdb.connect()

with duckdb.connect() as conn:
    conn.execute("""
        COPY df_estatisticas_canais_pandas
        TO '/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_canais'
        (FORMAT PARQUET, PARTITION_BY ('ASSUNTO', 'ANO_EXTRACAO', 'MES_EXTRACAO','DIA_EXTRACAO', 'TURNO_EXTRACAO'))
""")


df_videos = spark.read.parquet(
    '/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/prata/estatisticas_videos')
df_videos = df_videos.withColumn(
    'TURNO_EXTRACAO', F.when((col('DIA_EXTRACAO') == 20) &
                             (hour(col('DATA_EXTRACAO')) == 18), 'Tarde')
    .otherwise(col('TURNO_EXTRACAO'))
)


def convert_to_minutes(tempo):
    try:
        pattern = re.compile(r'PT(\d+H)?(\d+M)?(\d+S)?')
        match = pattern.match(tempo)
        hours = int(match.group(1)[:-1]) if match.group(1) else 0
        minutes = int(match.group(2)[:-1]) if match.group(2) else 0
        seconds = int(match.group(3)[:-1]) if match.group(3) else 0
        total_minutes = hours * 60 + minutes + seconds / 60
        return total_minutes
    except:
        return 0


# Aplicando a função ao dataframe
df_videos_pandas = df_videos.toPandas()
df_videos_pandas.head()
df_videos_pandas.info()
df_videos_pandas['DATA_EXTRACAO'] = df_videos_pandas['DATA_EXTRACAO'].astype(
    'datetime64[ns]')

df_videos_pandas[
    ['TITULO_VIDEO', 'DESCRICAO', 'DURACAO', 'ASSUNTO',
        'TURNO_EXTRACAO', 'ID_CANAL', 'ID_VIDEO']
] = df_videos_pandas[
    ['TITULO_VIDEO', 'DESCRICAO', 'DURACAO', 'ASSUNTO',
        'TURNO_EXTRACAO', 'ID_CANAL', 'ID_VIDEO']
].astype('string')
df_videos_pandas.fillna('0', inplace=True)
df_videos_pandas[
    ['ID_CATEGORIA', 'TOTAL_VISUALIZACOES', 'TOTAL_LIKES', 'TOTAL_FAVORITOS']
] = df_videos_pandas[
    ['ID_CATEGORIA', 'TOTAL_VISUALIZACOES', 'TOTAL_LIKES', 'TOTAL_FAVORITOS']
].astype('int64')
df_videos_pandas['DURACAO_VIDEO_MINUTOS'] = df_videos_pandas['DURACAO'].apply(
    convert_to_minutes)
df_videos_pandas['DURACAO_VIDEO_MINUTOS'] = df_videos_pandas['DURACAO_VIDEO_MINUTOS'].astype(
    'float64')
with duckdb.connect() as conn:
    conn.execute("""
        COPY df_videos_pandas
        TO '/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/ouro/estatisticas_videos'
        (FORMAT PARQUET, PARTITION_BY ('ASSUNTO', 'ANO_EXTRACAO', 'MES_EXTRACAO','DIA_EXTRACAO', 'TURNO_EXTRACAO', 'ID_CANAL', 'ID_VIDEO'))
""")
spark.stop()
