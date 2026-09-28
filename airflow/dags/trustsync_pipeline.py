from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'trustsync',
    'retries':2,
    'retry_delay': timedelta(minutes=2),
}

DBT_DIR = '/home/tomy/trustsync/proyectos/trustsync_dbt'
KAFKA_DIR = '/home/tomy/trustsync/kafka'
VENV = '/home/tomy/trustsync/trustenv/bin'

with DAG(
    dag_id='trustsync_pipeline',
    default_args=default_args,
    description='Pipeline TrustSync: kafka -> csv -> dbt',
    schedule='0 2 * * *',
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=['trustsync', 'kafka', 'dbt'],
) as dag:
    
    # Tarea 1 - Publicar eventos en kafka
    kafka_producer = BashOperator(
        task_id='kafka_producer',
        bash_command=(
            f'cd {KAFKA_DIR} && '
            f'export $(grep -v "^#" .env | xargs) && '
            f'python producer_cobros.py'
        ),
)

    kafka_consumer = BashOperator(
        task_id='kafka_consumer',
        bash_command=(
            f'cd {KAFKA_DIR} && '
            f'export $(grep -v "^#" .env | xargs) && '
            f'timeout 30 python consumer_cobros.py || [ $? -eq 124 ]'
            
        ),
    )

    pii_scan = BashOperator(
        task_id='presidio_pii_scan',
        bash_command=(
            f'cd {KAFKA_DIR} && '
            f'export $(grep -v "^#" .env | xargs) && '
            f'python pii_detector.py'
        ),
    )

    gx_valitation = BashOperator(
        task_id='great_validation',
        bash_command=(
            f'cd {KAFKA_DIR} && '
            f'export $(grep -v "^#" .env | xargs) && '
            f'python gx_validator.py'
        ),
    )
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && dbt test',
    )
    

    
    kafka_producer >> kafka_consumer >> pii_scan >> gx_valitation >> dbt_run >> dbt_test
    
    