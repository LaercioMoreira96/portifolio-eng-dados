# 🎵 Pipeline Spotify PySpark

**Pipeline ETL educacional** de processamento de dados com Apache Spark, demonstrando boas práticas de engenharia de dados em produção.

## 🎯 Objetivo

Exemplificar o processamento distribuído de dados em larga escala utilizando a arquitetura **Medallion** (Bronze → Silver → Gold), aplicando transformações, validações e otimizações tipicamente encontradas em pipelines reais.

## 📋 O que faz

- 🔄 **Extração (Bronze):** Leitura de dados de múltiplas fontes e formatos
- 🛠️ **Validação (Silver):** Limpeza, normalização e tratamento de dados
- 💾 **Agregação (Gold):** Cálculos, métricas e estruturação para consumo
- 📊 **Otimizações:** Particionamento, caching, compressão Parquet
- ✅ **Qualidade:** Validações de integridade e detectação de anomalias

## 🏗️ Arquitetura Medallion

```
Raw Data (JSON, CSV, APIs)
    ↓
[BRONZE LAYER]
├─ Armazenamento bruto
├─ Validação mínima
└─ Rastreamento de origem
    ↓
[SILVER LAYER]
├─ Deduplicação
├─ Normalização
├─ Enriquecimento de dados
└─ Validações de qualidade
    ↓
[GOLD LAYER]
├─ Agregações e transformações
├─ Cálculos analíticos
├─ Estrutura final otimizada
└─ Pronto para BI/Analytics
```

## 💡 Padrões Demonstrados

- ✅ **PySpark Catalyst:** Otimização automática de queries
- ✅ **Window Functions:** Cálculos de janelas e rankings
- ✅ **Particionamento:** Dividir dados por dimensão para performance
- ✅ **Lazy Evaluation:** Como Spark otimiza execução
- ✅ **UDFs:** User Defined Functions para lógica customizada
- ✅ **Performance Tuning:** Caching, broadcast, shuffle optimization

## 🛠️ Stack

| Componente | Versão |
|-----------|--------|
| **PySpark** | 3.x+ |
| **Python** | 3.8+ |
| **Apache Spark** | 3.x+ |
| **Pandas** | Para transformações complementares |
| **Jupyter** | Desenvolvimento e documentação |

## 📂 Estrutura

```
pipeline-spotify-pyspark/
│   
├── src/
│   └── pipeline_consolidada.py (pipeline principal)
├── data/
│   ├── bronze/    (dados brutos)
│   ├── silver/    (dados validados)
│   └── gold/      (dados prontos)
│   └── spotify-data.csv /      (dados brutos)
└── requirements.txt
```

## 🚀 Casos de Uso Reais

Este pipeline demonstra técnicas usadas em:
- **Data Lakes:** Processamento e organização em camadas
- **ETL em larga escala:** Múltiplos terabytes de dados
- **Real-time Analytics:** Agregações rápidas e eficientes
- **Machine Learning:** Preparação de dados para modelos
- **Business Intelligence:** Estruturação para dashboards

## 📊 Conhecimentos Aplicados

- Distributed computing com Spark
- SQL e otimização de queries
- Arquitetura de dados (Medallion)
- Processamento de grande volume
- Monitoramento e logging

## 👤 Autor

[Laércio Moreira](https://www.linkedin.com/in/laerciomoreira96) - Analista de Dados | Python | Spark
