from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="stock_etl_pipeline",
    start_date=datetime(2024, 6, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=(
            "docker exec spark-jupyter spark-submit "
            "/home/jovyan/work/scripts/e.py"
        )
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=(
            "docker exec spark-jupyter spark-submit "
            "/home/jovyan/work/scripts/t.py"
        )
    )

    load = BashOperator(
        task_id="load",
        bash_command=(
            "docker exec spark-jupyter spark-submit "
            "--packages net.snowflake:spark-snowflake_2.12:3.1.8,net.snowflake:snowflake-jdbc:3.13.30 "
            "/home/jovyan/work/scripts/l.py"
        )
    )

    extract >> transform >> load
