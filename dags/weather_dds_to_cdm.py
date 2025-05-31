# 1. section imports
import time
import logging as _log
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

#2. section global vars
PG_CONN_ID = "pg_weather"

LOAD_CDM_SQL = '''
call load_cdm_dim_city();
call load_cdm_dim_weather_condition();
call load_cdm_fact_weather({cdm_dag_schedule_hour_utc});
call load_cdm_fact_weather_daily({cdm_dag_schedule_hour_utc});
'''

# 3. section def
def load_data_to_cdm(**context):
    """
    Запись данных в базу данных в слой CDM
    """
    current_hour_utc = time.gmtime()[3]

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(LOAD_CDM_SQL.format(cdm_dag_schedule_hour_utc=current_hour_utc))

    conn.commit()
    conn.close()

# 4. section dag, task descr
args = {
    "owner": "airflow",
    "catchup": "False",
}

with DAG(
    dag_id="weather_dds_to_cdm",
    default_args=args,
    description="Loading data from DDS to CDM",
    start_date=datetime(2025, 5, 28, 22, 15, 0),
    schedule_interval="15 17,22 * * *", # Каждый день в 17:15 UTC (00:15 следующего дня по Красноярску, 01:15 по Иркутску, 02:15 по Якутску, 03:15 по Владивостоку, 04:15 по Магадану, 05:15 по Петропавловску-Камчатскому)
                                        #           и в 22:15 UTC (00:15 следующего дня по Калининграду, 01:15 по Москве, 02:15 по Саратову, 03:15 по Екатеринбургу, 04:15 по Омску)
    tags=["pg", "API"],
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    load_data_to_cdm_task = PythonOperator(
        task_id="load_data_to_cdm_task",
        python_callable=load_data_to_cdm,
        provide_context=True,
        dag=dag
    )

    load_data_to_cdm_task