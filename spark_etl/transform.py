import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f


def abrir_dataframe(spark: SparkSession) -> DataFrame:
    dataframe = spark.read.json(
        '../datalake/bronze/cities_skylines/extracao_data_2024_06_29/estatisticas_canais_brasileiros/req_estatisticas_canais_brasileiros.json')
    return dataframe


def obter_hora(dataframe: DataFrame) -> DataFrame:
    dataframe = dataframe.withColumn('TURNO_EXTRACAO', f.when((f.col('HORA') >= 0) & (f.col('HORA') < 6), 'Madrugada')
                                     .when((f.col('HORA') >= 6) & (f.col('HORA') < 12), 'Manhã')
                                     .when((f.col('HORA') >= 12) & (f.col('HORA') < 18), 'Tarde')
                                     .otherwise('Noite'))
    return dataframe


def transformar_estatisticas_canais(dataframe: DataFrame) -> DataFrame:
    dataframe = dataframe.select(
        'data_extracao',
        f.explode('items').alias('item')
    ).withColumn('HORA', f.hour('data_extracao')) \
        .select(
        f.col('data_extracao').alias('DATA_EXTRACAO'),
        f.col('TURNO_EXTRACAO'),
        f.col('HORA'),
        f.col('item.id').alias('ID'),
        f.col('item.snippet.title').alias('NM_CANAL'),
        f.col('item.statistics.subscriberCount').alias('TOTAL_INSCRITOS'),
        f.col('item.statistics.videoCount').alias('TOTAL_VIDEO_PUBLICADO'),
        f.col('item.statistics.viewCount').alias('TOTAL_VISUALIZACOES'),
    )
    dataframe = obter_hora(dataframe)
    return dataframe


def transform_estatisticas_videos(df_video_json: DataFrame) -> DataFrame:
    df_video_json = df_video_json.select(
        'data_extracao',
        f.col('items.id').alias('ID_VIDEO'),
        f.col('items.snippet.publishedAt').alias('DATA_PUBLICACAO'),
        f.col('items.snippet.channelId').alias('ID_CANAL'),
        f.col('items.snippet.channelTitle').alias('NM_CANAL'),
        f.col('items.snippet.categoryId').alias('ID_CATEGORIA'),
        f.col('items.snippet.title').alias('TITULO_VIDEO'),
        f.col('items.snippet.description').alias('DESCRICAO'),
        f.col('items.snippet.tags').alias('TAGS'),
        f.col('items.contentDetails.duration').alias('DURACAO_VIDEOS'),
        f.col('items.statistics.viewCount').alias('TOTAL_VISUALIZACOES'),
        f.col('items.statistics.likeCount').alias('TOTAL_LIKES'),
        f.col('items.statistics.favoriteCount').alias('TOTAL_FAVORITOS'),
        f.col('items.statistics.commentCount').alias('TOTAL_COMENTARIOS'),
    )
    return df_video_json


def transform_estatisticas_videos_trends(df_trend_brazil: DataFrame) -> DataFrame:
    df_trend_brazil = df_trend_brazil.drop('_corrupt_record')
    df_trend_brazil = df_trend_brazil.na.drop('all')
    df_trend_brazil = df_trend_brazil.select(
        'data_extracao',
        f.explode('items')
    )
    df_trend_brazil = df_trend_brazil.select(
        'data_extracao',
        f.col('col.id'),
        f.col('col.contentDetails.*'),
        f.col('col.snippet.*'),
        f.col('col.statistics.*')
    ).select(
        'data_extracao',
        f.col('categoryId').alias('ID_CATEGORIA'),
        f.col('channelId').alias('ID_CANAL'),
        f.col('channelTitle').alias('NM_CANAL'),
        f.col('id').alias('ID_VIDEO'),
        f.col('title').alias('TITULO_VIDEO'),
        f.col('duration').alias('DURACAO'),
        f.col('description').alias('DESCRICAO'),
        f.col('commentCount').alias('TOTAL_COMENTARIOS'),
        f.col('favoriteCount').alias('TOTAL_FAVORITOS'),
        f.col('likeCount').alias('TOTAL_LIKES'),
        f.col('viewCount').alias('TOTAL_VISUALIZACOES'),
    )
    return df_trend_brazil


def save_parquet(df_json: DataFrame, diretorio_salvar: str):
    df_json.coalesce(1) \
        .write \
        .mode('overwrite')\
        .parquet(diretorio_salvar)


if __name__ == '___main__':
    pass
