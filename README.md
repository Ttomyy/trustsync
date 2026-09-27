# TrustSync

DEMO Orquestación Ética y Privacidad Automatizada en Pipelines de Datos.
obtenemos los datos de un producer "randomm" de confluent kafka, los leemos y los cargamos en Mongodb
despúes leemos datos y detectamos datos PII para anonimizarlos y actualizarlos (capa bronce)


## Stack
- **DBT + DuckDB** — transformación y tests de calidad (capa silver)
- **Kafka / Confluent** — streaming de eventos de cobros
- **Airflow** — orquestación del pipeline completo

## Estructura


## Setup rápido
```bash
pip install -r requirements.txt
cp kafka/.env.example kafka/.env  # añade tus credenciales
cd dbt && dbt build
```
