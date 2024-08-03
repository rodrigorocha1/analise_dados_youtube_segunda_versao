import pendulum
import argparse
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import duckdb
import os


def abrir_dataframe(spark: SparkSession, caminho: str) -> DataFrame:
    dataframe = spark.read.json(caminho)
    return dataframe


def obter_turno(data_extracao_col: F.col):
    return F.when((F.hour(data_extracao_col) >= 0) & (F.hour(data_extracao_col) < 6), 'Madrugada') \
            .when((F.hour(data_extracao_col) >= 6) & (F.hour(data_extracao_col) < 12), 'Manhã') \
            .when((F.hour(data_extracao_col) >= 12) & (F.hour(data_extracao_col) < 18), 'Tarde') \
            .otherwise('Noite')


def fazer_tratamento_canais(dataframe: DataFrame) -> DataFrame:
    dataframe = dataframe.select(
        'data_extracao',
        'assunto',
        F.explode('items').alias('items')
    ).select(
        F.col('assunto').alias('ASSUNTO'),
        F.col('data_extracao').alias('DATA_EXTRACAO'),
        F.year('data_extracao').alias('ANO_EXTRACAO'),
        F.month('data_extracao').alias('MES_EXTRACAO'),
        F.dayofmonth('data_extracao').alias('DIA_EXTRACAO'),
        obter_turno(F.col('data_extracao')).alias('TURNO_EXTRACAO'),
        F.col('items.id').alias('ID_CANAL'),
        F.col('items.snippet.title').alias('NM_CANAL'),
        F.col('items.statistics.subscriberCount').alias('TOTAL_INSCRITOS'),
        F.col('items.statistics.videoCount').alias('TOTAL_VIDEOS_PUBLICADOS'),
        F.col('items.statistics.viewCount').alias('TOTAL_VISUALIZACOES')
    )
    return dataframe


def fazer_tratamento_video(dataframe: DataFrame) -> DataFrame:
    dataframe = dataframe.select('data_extracao', 'assunto', F.explode('items').alias('items')) \
        .select(
        F.col('assunto').alias('ASSUNTO'),
        F.col('data_extracao').alias('DATA_EXTRACAO'),
        F.year('data_extracao').alias('ANO_EXTRACAO'),
        F.month('data_extracao').alias('MES_EXTRACAO'),
        F.dayofmonth('data_extracao').alias('DIA_EXTRACAO'),
        obter_turno(F.col('data_extracao')).alias('TURNO_EXTRACAO'),
        F.col('items.id').alias('ID_VIDEO'),
        F.col('items.snippet.channelId').alias('ID_CANAL'),
        F.col('items.snippet.title').alias('TITULO_VIDEO'),
        F.col('items.snippet.description').alias('DESCRICAO'),
        F.col('items.contentDetails.duration').alias('DURACAO'),
        F.col('items.snippet.tags').alias('TAGS'),

        F.col('items.snippet.categoryid').alias('ID_CATEGORIA'),
        F.col('items.statistics.viewCount').alias('TOTAL_VISUALIZACOES'),
        F.col('items.statistics.likeCount').alias('TOTAL_LIKES'),
        F.col('items.statistics.favoriteCount').alias('TOTAL_FAVORITOS'),
    )
    dataframe = dataframe.withColumn('TOTAL_TAGS', F.when(
        F.size(dataframe.TAGS) <= 0, 0).otherwise(F.size(dataframe.TAGS)))
    dataframe = dataframe.withColumn(
        'TOTAL_PALAVRAS_TITULO', F.size(F.split(dataframe.TITULO_VIDEO, " ")))
    dataframe = dataframe.withColumn(
        'TOTAL_PALAVRAS_DESCRICAO', F.size(F.split(dataframe.DESCRICAO, " ")))
    return dataframe


def salvar_dados_particionados(dataframe: DataFrame, caminho_completo: str, particoes: Tuple[str]):
    print(dataframe.show())
    dataframe.write.mode("append").partitionBy(
        particoes).parquet(caminho_completo)


if __name__ == "__main__":
    print('______iniciando______________')
    caminho_base = os.getcwd()
    parser = argparse.ArgumentParser(
        description='ETL YOUTUBE')
    parser.add_argument('--opcao', type=str, required=True,
                        help='Opcao para obter a métrica')
    parser.add_argument('--caminho_arquivo', type=str, required=True,
                        help='camihno do arquivo')
    args = parser.parse_args()
    caminho_arquivo = args.caminho_arquivo
    opcao = args.opcao
    # caminho_arquivo = '/home/rodrigo/Documentos/projetos/analise_dados_youtube_segunda_versao/datalake/bronze/*/extracao_data_2024_07_15_noite/estatisticas_canais_brasileiros/req_estatisticas_canais_brasileiros.json'
    # opcao = 'C'
    spark = SparkSession.builder.appName("criar_dataframe").getOrCreate()

    dataframe = abrir_dataframe(
        spark, caminho_arquivo)
    if opcao == 'C':
        dataframe = fazer_tratamento_canais(dataframe)
        caminho_arquivo = os.path.join(
            caminho_base, 'datalake', 'prata', 'estatisticas_canais')
        nome_arquivo = 'estatisticas_canais.parquet'
        particoes = "ASSUNTO", "ANO_EXTRACAO", "MES_EXTRACAO", "DIA_EXTRACAO", "TURNO_EXTRACAO"

    else:
        dataframe = fazer_tratamento_video(dataframe)
        caminho_arquivo = os.path.join(
            caminho_base, 'datalake', 'prata', 'estatisticas_videos')
        particoes = "ASSUNTO", "ANO_EXTRACAO", "MES_EXTRACAO", "DIA_EXTRACAO", "TURNO_EXTRACAO", "ID_CANAL", "ID_VIDEO"
        nome_arquivo = 'estatisticas_videos.parquet'
    caminho_completo = os.path.join(caminho_arquivo, nome_arquivo)
    os.makedirs(caminho_arquivo, exist_ok=True)
    print(dataframe.show())
    salvar_dados_particionados(
        dataframe=dataframe, caminho_completo=caminho_completo, particoes=particoes)
    spark.stop()
