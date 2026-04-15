import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# --- CONFIGURAÇÃO DE LOGGING ---
# Define que os logs vão para o arquivo e também aparecem na tela
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("pipeline_spotify.log"), logging.StreamHandler()]
)
logger = logging.getLogger("SpotifyPipeline")

start_time = time.time()

try:
    # --- INÍCIO DO PROCESSO ---
    logger.info("Iniciando pipeline de dados...")

    # Carregar username e token
    logger.info("Carregando credenciais...")
    with open('credenciais.txt', 'r') as file:
        linhas = file.readlines()
        os.environ['KAGGLE_USERNAME'] = linhas[0].strip()
        os.environ['KAGGLE_KEY'] = linhas[1].strip()

    # Download do dataset
    logger.info("Baixando dataset do Kaggle...")
    os.system("kaggle datasets download -d kapturovalexander/spotify-data-from-pyspark-course -p ./data --unzip")

    # ABRIR SESSÃO SPARK
    logger.info("Iniciando Sessão Spark...")
    spark = SparkSession.builder \
        .appName("EstudoSpark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    logger.info(f"Sessão Spark Ativa! Versão: {spark.version}")

    # CARREGAR DADOS BRUTOS .CSV
    logger.info("Lendo arquivo CSV...")
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .load('./data/spotify-data.csv')

    # --- CAMADA BRONZE ---
    logger.info("Processando Camada Bronze...")
    df.withColumn("data_ingestao", f.current_timestamp()) \
      .withColumn("data_particao", f.to_date(f.current_timestamp())) \
      .write \
      .mode("append") \
      .partitionBy("data_particao") \
      .parquet("data/bronze/spotify")

    # --- CAMADA SILVER ---
    logger.info("Processando Camada Silver...")
    colunas_int = ['year', 'key',  'popularity']
    colunas_decimais = ['acousticness', 'danceability', 'energy', 'instrumentalness', 
                      'liveness', 'loudness', 'speechiness', 'tempo', 'mode', 'valence']

    for col in colunas_int:
        df = df.withColumn(col, f.col(col).cast("INT"))

    for col in colunas_decimais:
        df = df.withColumn(col, f.col(col).cast("DOUBLE"))
        
    # Limpando sujeiras da coluna do nome do artista
    df = df.withColumn("artists", f.split(f.regexp_replace(f.col("artists"), r"[\[\]']", ""), ", "))

    # Conversão ms para seg
    df = df.withColumn("explicit", f.col("explicit").cast("BOOLEAN"))\
        .withColumn("duracao_segundos", f.round(f.col("duration_ms") / 1000).cast("INT"))\
        .drop("duration_ms")
        
    # Dicionário de tradução
    colunas_traduzidas = {
        "name": "nome_da_musica",
        "artists": "nome_dos_artistas",
        "release_date": "data_de_lancamento",
        "year": "ano",
        "acousticness": "acustividade",
        "danceability": "dancabilidade",
        "energy": "energia",
        "instrumentalness": "instrumentalidade",
        "liveness": "vivacidade",
        "loudness": "volume",
        "speechiness": "fala",
        "tempo": "ritmo",
        "valence": "positividade",
        "mode": "modo",
        "key": "tonalidade",
        "popularity": "popularidade",
        "explicit": "explicito"
    }

    for antigo, novo in colunas_traduzidas.items():
        df = df.withColumnRenamed(antigo, novo)
        
    # Removi a data_de_lançamento porque não está padronizada, tem linhas com ano-mês-dia e outras só com ano.
    df = df.drop("data_de_lancamento")

    logger.info("Salvando Camada Silver...")
    df.withColumn("data_ingestao", f.current_timestamp()) \
      .withColumn("data_particao", f.to_date(f.current_timestamp())) \
      .write \
      .mode("overwrite") \
      .partitionBy("data_particao") \
      .parquet("data/silver/spotify")

    # --- CAMADA GOLD ---
    logger.info("Processando Camada Gold...")
    # Expandir os artistas em linhas separadas, pois pode ter mais de um artista por música
    df = df.withColumn("nome_do_artista", f.explode(f.col("nome_dos_artistas")))
   
    # Metricas agregadas por artista, em geral médias
    media_popularidade = f.round(f.avg("popularidade"), 2).alias("popularidade")
    media_energia = f.round(f.avg("energia"), 2).alias("energia")
    media_ritmo = f.round(f.avg("ritmo"), 2).alias("ritmo")
    total_de_musicas = f.count("nome_da_musica").alias("qtd_musicas")

    df = df.groupBy("nome_do_artista") \
        .agg(
            total_de_musicas,
            media_popularidade,
            media_energia,
            media_ritmo
        )
    # Filtrar artistas com mais de 5 músicas e ordenar por popularidade decrescente
    df = df.filter(f.col("qtd_musicas") > 5) \
        .orderBy(f.desc("popularidade"))

    logger.info("Salvando Camada Gold...")
    df.write \
        .mode("overwrite") \
        .parquet("data/gold/spotify_performance_artistas")

    logger.info("Pipeline finalizado com SUCESSO!")
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Processo finalizado. Duração: {duration:.2f} segundos.")
except Exception as e:
    # --- TRATAMENTO DE ERRO ---
    # Aqui o erro é capturado e salvo no arquivo com detalhes 
    logger.error(f"FALHA CRÍTICA NO PIPELINE: {e}", exc_info=True)
    raise # Relança o erro para parar a execução
