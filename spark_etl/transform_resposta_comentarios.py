import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f


def transform_resposta_comentarios(df_comentarios_json: DataFrame):
    df_comentarios_json.select(
        f.col('data_extracao').alias('DATA_EXTRACAO'),
        f.explode('items').alias('ITEMS')
    ) \
        .select(
            f.col('DATA_EXTRACAO'),
            f.col('ITEMS.id').alias('ID_RESPOSTA_COMENTARIOS'),
            f.col('ITEMS.snippet.channelId').alias('ID_CANAL'),
            f.col('ITEMS.snippet.textDisplay').alias('TEXTO'),
            f.col('ITEMS.snippet.likeCount').alias('TOTAL_LIKES'),
            f.col('ITEMS.snippet.publishedAt').alias('DATA_PUBLICACAO'),
            f.col('ITEMS.snippet.updatedAt').alias('DATA_ATUALIZACAO'),
    )
    return df_comentarios_json


def save_parquet(df_comentarios_json: DataFrame, diretorio_salvar: str):
    df_comentarios_json.coalesce(1) \
        .write \
        .mode('overwrite')\
        .parquet(diretorio_salvar)


def transform_youtube_video(spark: SparkSession, **kwargs):
    caminho_base = '/home/rodrigo/projetos/dados_youtube/analise_dados_youtube/data/projeto_youtube'

    caminho_load = os.path.join(
        caminho_base,
        '/bronze/',
        kwargs['load_assunto'],
        kwargs['load_path_extracao'],
        kwargs['load_metrica'],
        kwargs['load_arquivo']
    )
    df_resposta = spark.read.json(caminho_load)
    df_resposta = transform_resposta_comentarios(df_resposta)
    diretorio_save = os.path.join(
        caminho_base,
        '/prata/',
        kwargs['save_assunto'],
        kwargs['save_path_arquivo'],
        kwargs['save_metrica'],
        kwargs['save_nome_arquivo']
    )
    save_parquet(df_resposta, diretorio_save)