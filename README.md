# TrustSync

Orquestación Ética y Privacidad Automatizada en Pipelines de Datos.

## Stack
- **DBT + DuckDB** — transformación y tests de calidad
- **Kafka / Confluent** — streaming de eventos de cobros
- **Airflow** — orquestación del pipeline completo

## Estructura


## Setup rápido
```bash
pip install -r requirements.txt
cp kafka/.env.example kafka/.env  # añade tus credenciales
cd dbt && dbt build
```
