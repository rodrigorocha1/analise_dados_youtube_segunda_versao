import pendulum
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext


def criar_dataframe(spark: SparkSession) -> DataFrame:
    dados = [
        {"nome": "Alice", "idade": 30, "cidade": "São Paulo"},
        {"nome": "Bob", "idade": 25, "cidade": "Rio de Janeiro"},
        {"nome": "Charlie", "idade": 35, "cidade": "Belo Horizonte"}
    ]

    print('dentro da ')
    # Inicializar a sessão Spark
    print(dados)

    # Criar o DataFrame
    df = spark.createDataFrame(dados)

    print(df)
    return df



spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

print('dentro de main etl')
df = criar_dataframe(spark=spark)
df.write.parquet('teste.parquet')
print(df)
