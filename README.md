![DBT CI](https://github.com/Ttomyy/trustsync/actions/workflows/dbt_ci.yml/badge.svg)
# TrustSync — Data Governance & Automated Privacy Pipeline

PoC enfocada en **Gobierno del Dato, Privacidad Automatizada (GDPR) y Calidad** en flujos analíticos del sector asegurador. 

El proyecto implementa un pipeline *end-to-end* que traslada principios consolidados de arquitectura de integración empresarial (ETL/ELT tradicional) hacia el *Modern Data Stack*. El flujo automatiza la ingesta continua de eventos, almacenamiento operacional, ofuscación activa de PII y modelado dimensional analítico respaldado por pruebas de calidad.

---

## 🛠️ Stack Tecnológico

| Capa | Tecnología | Propósito |
| :--- | :--- | :--- |
| **Streaming** | Confluent Cloud (Kafka) | Ingesta desacoplada de eventos transaccionales de cobros (`DB`, `CB`, `PB`, `PA`). |
| **Bronze Layer** | MongoDB Atlas | Almacenamiento no relacional del flujo transaccional en bruto. |
| **Privacy / PII** | Microsoft Presidio + spaCy (`es_core_news_lg`) | Detección y enmascaramiento de datos personales sensibles (NIF/DNI y teléfonos españoles). |
| **Silver / Marts** | dbt Core + DuckDB | Modelado analítico SQL (staging y hechos), window functions y validación de reglas de negocio. |
| **Orquestación** | Apache Airflow (Docker) | Automatización, monitorización y manejo de dependencias mediante un DAG resiliente. |

---

## 🏗️ Arquitectura y Flujo de Datos

```text
[Kafka Producer] 
       │ (Streaming de cobros)
       ▼
[Kafka Consumer] ──► [MongoDB Atlas: Capa Bronze]
                            │
                     [Presidio PII Scanner] ──► [MongoDB: Colección Anonimizada]
                                                        │
                                              [dbt Core + DuckDB]
                                              ├── stg_kafka_cobros (Vistas / Limpieza)
                                              └── fct_cobros_validados (Modelo Core)
                                                        │
                                              [7 Tests Automatizados (PASS)]
