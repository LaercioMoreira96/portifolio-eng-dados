# 📊 Pipeline Cálculo de Indicadores de Atendimento (Camada Gold)

**Pipeline de processamento em PySpark/SQL** para transformação de dados brutos de call center em indicadores analíticos estruturados.

## 📋 O que faz

- 🔄 Leitura de dados de múltiplas fontes (interações, chamadas, chats)
- 🛠️ Transformações complexas usando SQL Spark e window functions
- 💾 Cálculo de métricas de atendimento por dimensão (célula, empresa, período)
- 📊 Agregações com múltiplos níveis: diário, semanal, mensal
- ✅ Validações de integridade e consistência de dados
- 📁 Escrita otimizada em Parquet (camada Gold - pronto para BI)

## 📈 Métricas Calculadas

- ✅ Volume de interações recebidas
- ✅ Tempo médio de operação (TMO)
- ✅ Nível de serviço (NS%)
- ✅ Taxa de abandono
- ✅ Posição de atendimento (PA)
- ✅ Análise de produtividade por agente/célula

## 🏗️ Arquitetura

```
Raw Layer (Dados brutos)
    ↓
Refined Layer (Validação básica)
    ↓
[Transformações Spark/SQL]
    ↓
Gold Layer (Indicadores prontos)
    ↓
Power BI / Dashboards
```

## 💡 Casos de Uso

- 📈 Dashboards de performance em tempo real
- 📊 Relatórios gerenciais de KPIs
- 🎯 Análise de dimensionamento e capacidade
- 📋 Benchmarking entre células/equipes
- 🔍 Investigação de anomalias de atendimento

## 🛠️ Stack

- **PySpark** - Processamento distribuído de dados
- **SQL Spark** - Queries otimizadas e window functions
- **Pandas** - Transformações complementares
- **Jupyter Notebook** - Desenvolvimento interativo

## ⚙️ Como usar

### 1. Pré-requisitos

```bash
pip install -r requirements.txt
```

### 2. Executar notebook

```bash
jupyter notebook notebooks/portifolio-exemplo-pipeline-sql_pyspark-gold-calculo-indicadores.ipynb
```

### 3. Outputs

Arquivo Parquet estruturado com indicadores de atendimento, pronto para:
- ✅ Conexão em Power BI
- ✅ Análises SQL adicionais
- ✅ Feeds de relatórios automáticos

## 📊 Exemplo de Dados Processados

| data | categoria | volume_recebido | TMO | NS_pct | PA_calculado |
| --- | --- | --- | --- | --- | --- |
| 2026-04-14 | Atendimento N1 | 150 | 5.2 | 95 | 12 |
| 2026-04-14 | Suporte Técnico | 45 | 4.8 | 92 | 5 |
| 2026-04-14 | Atendimento N2 | 78 | 6.1 | 88 | 8 |

## 🚀 Benefícios

- ⏱️ **Performance:** Processa milhões de registros em minutos
- 🎯 **Precisão:** Indicadores confiáveis para decisão
- 📊 **Escalabilidade:** Adiciona novas métricas facilmente
- 🔄 **Reutilizável:** Estrutura pronta para múltiplos períodos

## 👤 Autor

[Laércio Moreira](https://www.linkedin.com/in/laerciomoreira96)
