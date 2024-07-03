import os
from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f


def abrir_dataframe(spark: SparkSession, camada_datalake: str, assunto: str, extracao_data: str, metrica: str, arquivo_json: str) -> DataFrame:
    """Função para abrir o dataframe

    Args:
        spark (SparkSession): Sessão Spark
        camada_datalake (str): Camada datalake (bronze, prata, ouro)
        extracao_data (str): diretório com a pasta com o nome da extração ex: extracao_data_2024_07_02_15_05_38
        metrica (str): métrica Ex. estatisticas_canais_brasileiros, estatistica videos
        arquivo_json (str): nome do arquivo json

    Returns:
        DataFrame: Dataframe sparl
    """
    CAMINHO_RAIZ = os.getcwd()

    dataframe = spark.read.json(os.path.join(
        CAMINHO_RAIZ, 'datalake', camada_datalake, assunto, extracao_data, metrica, arquivo_json))
    # dataframe = spark.read.json(
    #     f'../datalake/{camada_datalake}/{assunto}/{extracao_data}/{metrica}/{arquivo_json}')

    return dataframe


def obter_hora(dataframe: DataFrame) -> DataFrame:
    """função para converter hora

    Args:
        dataframe (DataFrame): Dataframe pyspark

    Returns:
        DataFrame: Retorna um dataframe
    """
    dataframe = dataframe.withColumn('TURNO_EXTRACAO', f.when((f.col('HORA') >= 0) & (f.col('HORA') < 6), 'Madrugada')
                                     .when((f.col('HORA') >= 6) & (f.col('HORA') < 12), 'Manhã')
                                     .when((f.col('HORA') >= 12) & (f.col('HORA') < 18), 'Tarde')
                                     .otherwise('Noite'))
    return dataframe


def transformar_estatisticas_canais(dataframe: DataFrame) -> DataFrame:
    """transformar_estatisticas_canais

    Args:
        dataframe (DataFrame): Dataframe pyspark

    Returns:
        DataFrame: Retorna um dataframe
    """
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
    """transform_estatisticas_videos

    Args:
        df_video_json (DataFrame): Dataframe

    Returns:
        DataFrame: Dataframe
    """
    df_video_json = df_video_json.withColumn('HORA', f.hour('data_extracao')).select(
        'data_extracao',
        f.col('items.id').alias('ID_VIDEO'),
        f.col('items.snippet.publishedAt').alias('DATA_PUBLICACAO'),
        f.col('HORA'),
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
    """transform_estatisticas_videos_trends

    Args:
        df_trend_brazil (DataFrame): DataFrame

    Returns:
        DataFrame: DataFrame
    """
    df_trend_brazil = df_trend_brazil.drop('_corrupt_record')
    df_trend_brazil = df_trend_brazil.na.drop('all')
    df_trend_brazil = df_trend_brazil.withColumn('HORA', f.hour('data_extracao')).select(
        'data_extracao',
        f.explode('items')
    )
    df_trend_brazil = df_trend_brazil.select(
        'data_extracao',
        f.col('HORA'),
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


def save_parquet(dataframe: DataFrame, camada_datalake: str, assunto: str, extracao_data: str, metrica: str, nome_arquivo_parquet: str):
    """Salvar o arquivo parquet na camada prata

    Args:
        dataframe (DataFrame): Dataframe do pandas
        assunto (str): assunto da pesquisa
        extracao_data (str): diretório com a pasta com o nome da extração ex: extracao_data_2024_07_02_15_05_38
        metrica (str): métrica Ex. estatisticas_canais_brasileiros, estatistica videos
        nome_arquivo_parquet (str): nome do arquivo a ser salvo no parquet ex: teste.parquet
    """
    CAMINHO_RAIZ = os.getcwd()
    CAMINHO_DATALAKE_PRATA = os.path.join(
        CAMINHO_RAIZ, 'datalake', camada_datalake, assunto, extracao_data, metrica)
    os.makedirs(CAMINHO_DATALAKE_PRATA, exist_ok=True)
    
    dataframe.toPandas().to_parquet(
        path=os.path.join(CAMINHO_DATALAKE_PRATA, nome_arquivo_parquet))

    # df_json.coalesce(1) \
    #     .write \
    #     .mode('overwrite')\
    #     .parquet(diretorio_salvar)


def realizar_etl(spark: SparkSession, caminhos_datalake: List[Dict[str, str]], nome_arquivo_parquet: str):

    for caminho_datalake in caminhos_datalake:
        caminho_datalake['spark'] = spark
        dataframe = abrir_dataframe(**caminho_datalake)
        if caminho_datalake['metrica'] == 'estatisticas_canais_brasileiros':
            dataframe = transformar_estatisticas_canais(dataframe=dataframe)
        else:
            dataframe = transform_estatisticas_videos(df_video_json=dataframe)

        dataframe = obter_hora(dataframe=dataframe)
        caminho_datalake['dataframe'] = dataframe
        caminho_datalake['camada_datalake'] = 'prata'
        del caminho_datalake['spark']
        del caminho_datalake['arquivo_json']
        caminho_datalake['nome_arquivo_parquet'] = nome_arquivo_parquet
        print(caminho_datalake)
        save_parquet(**caminho_datalake)


if __name__ == '__main__':
    # import argparse
    # parser = argparse.ArgumentParser(
    #     description='Spark Youtube api'
    # )
    # # camada_datalake: str, extracao_data: str, metrica: str, arquivo_json: str
    # parser.add_argument('--camada_datalake', required=True)
    # parser.add_argument('--extracao_data', required=True)
    # parser.add_argument('--metrica', required=True)
    # parser.add_argument('--arquivo_json', required=True)
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()
    # args = parser.parse_args()
    camada_datalake = 'bronze'
    assunto = 'cities_skylines'
    extracao_data = 'extracao_data_2024_06_29'
    metrica = 'estatisticas_videos'
    arquivo_json = 'req_estatisticas_videos.json'
    nome_arquivo_parquet = 'req_estatisticas_videos.parquet'
    caminhos_datalake = [
        {
            'camada_datalake': camada_datalake,
            'assunto': assunto,
            'extracao_data': extracao_data,
            'metrica': metrica,
            'arquivo_json': arquivo_json
        }
    ]
    realizar_etl(spark=spark, caminhos_datalake=caminhos_datalake,
                 nome_arquivo_parquet=nome_arquivo_parquet)
