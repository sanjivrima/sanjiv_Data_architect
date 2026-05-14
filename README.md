## Overview
End-to-end data pipeline using Spark + Delta Lake + RAG-based LLM chatbot.

## Architecture
data_to_llm_architecture.pptx

## Tech Stack
- PySpark
- Delta Lake
- Python
- FAISS (RAG)
- OpenAI / LLM
- FastAPI

## How to Run Pipeline
 1.Bronze Layer
python etl/bronze/electricity_bronze.py
2. Silver Layer
 python etl/silver/electricity_silver.py
3. Gold Layer
 python etl/gold/electricity_gold.py

## Data Layers
 Bronze
  Raw API JSON
  Partitioned by ingestion date
Silver
  Clean structured tables
  Type enforced
  Deduplicated
Gold
  Business KPIs
  Daily aggregates
  Analytics-ready tables
