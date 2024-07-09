import argparse
from pyspark.sql import SparkSession


def criar_dataframe(spark: SparkSession, teste: int):
    dados = [
        {"nome": "Alice", "idade": 30, "cidade": "São Paulo"},
        {"nome": "Bob", "idade": 25, "cidade": "Rio de Janeiro"},
        {"nome": "Charlie", "idade": 35, "cidade": "Belo Horizonte"}
    ]

    print('Criando o DataFrame com os dados fornecidos')

    df = spark.createDataFrame(dados)

    print('DataFrame criado. Salvando em formato Parquet...')

    df.write.parquet('teste.parquet')

    print('Arquivo Parquet salvo com sucesso')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Script para criar DataFrame e salvar em Parquet')
    parser.add_argument('--teste', type=int, required=True,
                        help='Parâmetro de teste')

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("criar_dataframe") \
        .getOrCreate()

    criar_dataframe(spark, args.teste)

    spark.stop()
