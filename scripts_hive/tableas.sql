-------------------------------------------------------------------------

CREATE EXTERNAL TABLE TOTAL_VISUALIZACOES_POR_SEMANA(
	NM_CANAL string,
	TITULO_VIDEO string,
	TOTAL_CARACTERE_VIDEO INT,
	TAGS ARRAY<string>,
	DURACAO_VIDEO_MINUTOS FLOAT,
	TOTAL_TAGS INT,
	TOTAL_VISUALIZACOES INT,
	TOTAL_VISUALIZACOES_TURNO INT, 
	TOTAL_COMENTARIOS INT,
	TOTAL_COMENTARIOS_TURNO INT,
	TOTAL_LIKES INT,
	TOTAL_LIKES_TURNO INT
) 
PARTITIONED BY(
	ASSUNTO STRING, 
	data_extracao DATE, 
	ID_CANAL STRING, 
	ID_VIDEO string,
	TURNO_EXTRACAO VARCHAR(30)
	)
STORED AS PARQUET;





SELECT * 
FROM TOTAL_VISUALIZACOES_POR_SEMANA
where id_canal  = 'UC0MyjIBbIDlOBCUfl7iO9bg'

SHOW PARTITIONS  TOTAL_VISUALIZACOES_POR_SEMANA;

DESCRIBE FORMATTED TOTAL_VISUALIZACOES_POR_SEMANA;


SHOW TRANSACTIONS;

---------------------------------



CREATE EXTERNAL TABLE IF NOT EXISTS TOTAL_VIDEO_PUBLICADO_SEMANA (
    SEMANA_TRADUZIDA STRING,
    NM_CANAL STRING,
    TOTAL_VIDEOS INT
)
PARTITIONED BY( DATA_PUBLICACAO DATE,ASSUNTO STRING, ID_CANAL STRING)
STORED AS PARQUET;

LOAD DATA LOCAL INPATH '/home/rodrigo/Documentos/projetos/open_weather_api_apache/analise_dados_youtube/data/carga_hive/total_visualizacoes_semana_UC0MyjIBbIDlOBCUfl7iO9bg.parquet'
INTO TABLE TOTAL_VISUALIZACOES_POR_SEMANA;

SELECT * 
FROM TOTAL_VIDEO_PUBLICADO_SEMANA
where assunto  = 'assunto_cities_skylines';



--------------------------------------------------------------------------------------




CREATE TABLE zipcodes(
	RecordNumber int,
	Country string,
	City string,
	Zipcode int)
	PARTITIONED BY(state string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';


LOAD DATA INPATH 'hdfs://localhost:9000/teste/zipcodes20.csv' INTO TABLE zipcodes;



CREATE TABLE zipcodes_multiple(
RecordNumber int,
Country string,
City string)
PARTITIONED BY(state string, zipcode string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';


LOAD DATA INPATH 'hdfs://localhost:9000/teste/zipcodes20.csv' INTO TABLE zipcodes_multiple;

SELECT * FROM zipcodes ;

select * from zipcodes_multiple;


---------------------------------------------------------------------------------------------


CREATE EXTERNAL TABLE resposta_comentarios_youtube(
  `id_resposta_comentarios` string, 
  `texto` varchar(700))
PARTITIONED BY ( 
  `assunto` string, 
  `id_canal` string, 
  `id_video` string,
  `id_comentario` string)
STORED AS PARQUET;





LOAD DATA INPATH 'hdfs://localhost:9000/projeto/datalake_youtube/particao_por_campo/resposta_comentarios/resposta_comentarios_UCrOH1V-FyMunBIMrKL0y0xQ.parquet'
        INTO TABLE resposta_comentarios_youtube


SELECT   *
FROM resposta_comentarios_youtube
where assunto  = 'assunto_cities_skylines';

DESCRIBE FORMATTED resposta_comentarios_youtube;

---------------------------------------------------
CREATE EXTERNAL TABLE comentarios_youtube (
    ID_COMENTARIO STRING,
    TEXTO_COMENTARIO VARCHAR(700)
    
)
PARTITIONED BY(ASSUNTO STRING, ID_CANAL STRING, ID_VIDEO STRING)
STORED AS PARQUET;


MSCK REPAIR TABLE nome_da_tabela;


LOAD DATA INPATH 'hdfs://localhost:9000/projeto/datalake_youtube/comentarios.parquet' INTO TABLE comentarios_youtube;


select * from comentarios_youtube
where assunto  = 'assunto_cities_skylines';


DESCRIBE zipcodes;


DESCRIBE FORMATTED TOTAL_VIDEO_PUBLICADO_SEMANA;

SHOW PARTITIONS TOTAL_VIDEO_PUBLICADO_SEMANA;


----------------------------------------------------------------------------------

CREATE EXTERNAL TABLE TRENDS_YOUTUBE(
	NM_CANAL string,
  	TITULO_VIDEO string,
 	INDICE_TURNO_EXTRACAO TINYINT,
    TOTAL_VISUALIZACOES INTEGER ,
    TOTAL_FAVORITOS INTEGER,
    TOTAL_COMENTARIOS INTEGER,
	TOTAL_LIKES INTEGER,
	TOTAL_VISUALIZACOES_TURNO integer,
	TOTAL_FAVORITOS_TURNO integer, 
	TOTAL_COMENTARIOS_TURNO integer,
    TOTAL_LIKES_TURNO integer
)
PARTITIONED BY(
	data_extracao date,
	ID_CATEGORIA integer,
	ID_CANAL string,
	ID_VIDEO string, 
	TURNO_EXTRACAO string
)
STORED AS PARQUET;

SELECT * 
FROM TRENDS_YOUTUBE
where id_video  IN ('8KVsaoveTbw') ;


DESCRIBE FORMATTED TRENDS_YOUTUBE;

SHOW PARTITIONS TRENDS_YOUTUBE;

LOAD DATA LOCAL INPATH '/home/rodrigo/Documentos/projetos/open_weather_api_apache/analise_dados_youtube/data/carga_hive_trends/trends/trends_1.parquet'
INTO TABLE trends_youtube
---------------------------------------------------------------------------------


