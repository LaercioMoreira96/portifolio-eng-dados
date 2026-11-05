# 🎵 Pipeline Spotify PySpark

Pipeline ETL de estudo em **PySpark**, organizado na arquitetura **Medallion** (Bronze → Silver → Gold), usando um dataset público de músicas do Spotify disponível no Kaggle.

## 🎯 Objetivo

Praticar a estrutura de um pipeline em camadas: ingestão do dado bruto, limpeza e padronização, e uma tabela final agregada pronta para análise.

## 🔄 O que o pipeline faz

**1. Extração**
- Lê as credenciais do Kaggle de um arquivo local (fora do versionamento)
- Baixa o dataset [`spotify-data-from-pyspark-course`](https://www.kaggle.com/datasets/kapturovalexander/spotify-data-from-pyspark-course) via Kaggle CLI

**2. Bronze: dado bruto**
- Grava o CSV original em Parquet, sem transformações
- Adiciona `data_ingestao` e particiona por `data_particao`
- Modo `append`, mantendo o histórico de cargas

**3. Silver: dado limpo**
- Tipagem explícita das colunas (`INT`, `DOUBLE`, `BOOLEAN`)
- Limpeza da coluna de artistas (texto `"['A', 'B']"` → array `[A, B]`)
- Conversão da duração de milissegundos para segundos
- Tradução dos nomes das colunas para português
- Remoção de `release_date`, que não tem formato padronizado (algumas linhas trazem a data completa, outras só o ano)

**4. Gold: métricas por artista**
- `explode` do array de artistas (uma música com vários artistas conta para cada um)
- Agregação por artista: quantidade de músicas e média de popularidade, energia e ritmo
- Filtro de artistas com mais de 5 músicas, ordenado por popularidade

**Observabilidade**
- Logging em arquivo e no console, com a duração total da execução
- Em caso de erro, registra o stack trace e interrompe o processo

## 📂 Estrutura

```
pipeline-spotify-pyspark/
├── src/
│   └── pipeline_consolidada.py
├── requirements.txt
└── data/                         (gerado na execução, fora do Git)
    ├── spotify-data.csv
    ├── bronze/spotify/
    ├── silver/spotify/
    └── gold/spotify_performance_artistas/
```

## ▶️ Como executar

1. Instale as dependências: `pip install -r requirements.txt` (requer Java para o Spark)
2. Crie `credenciais.txt` na pasta de execução com o usuário do Kaggle na 1ª linha e a API key na 2ª
3. Execute: `python src/pipeline_consolidada.py`

## 🛠️ Stack

Python · PySpark · Parquet · Kaggle CLI

## 🔜 Próximos passos

- Testes de qualidade de dados (nulos, duplicidade, faixas válidas)
- Deduplicação na camada Silver
- Credenciais via variáveis de ambiente em vez de arquivo texto

## 👤 Autor

[Laércio Moreira](https://www.linkedin.com/in/laerciomoreira96)
