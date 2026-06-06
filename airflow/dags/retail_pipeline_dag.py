from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="retail_elt_pipeline",

    start_date=datetime(2026, 6, 5),

    schedule_interval="@daily",

    catchup=False,

    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1)
    }
) as dag:

    spark_cleaning = BashOperator(
        task_id="spark_cleaning",

        bash_command="""
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

        cd /opt/airflow/project

        python3 spark/jobs/data_cleaning.py
        """
    )

    load_postgres = BashOperator(
        task_id="load_postgres",

        bash_command="""
        cd /opt/airflow/project

        python3 postgres_loader/load_cleaned_data.py
        """
    )

    run_dbt = BashOperator(
        task_id="run_dbt",

        bash_command="""
        cd /opt/airflow/project/dbt/retail_transformations

        dbt run --profiles-dir /opt/airflow/project/dbt
        """
    )

    spark_cleaning >> load_postgres >> run_dbt