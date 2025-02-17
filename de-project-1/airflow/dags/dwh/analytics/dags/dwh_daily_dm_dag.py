import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from analytics.cdm.daily_datamarts import DailyDatamarts
from lib import ConnectionBuilder
from airflow.operators.python import get_current_context

log = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'de_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}
try:
    cdm_sql_path = Variable.get('cdm_sql_path')
except:
    cdm_sql_path = "/opt/airflow/dags/analytics/cdm/sql"

@dag(
    default_args=default_args,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 9, 1, tz="UTC"),  # Fixed start date
    catchup=False,
    tags=['daily', 'cdm', 'dwh'],
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        'start_date': (pendulum.now("UTC").subtract(days=1)).to_date_string(),
        'end_date': (pendulum.now("UTC").subtract(days=1)).to_date_string()
    }
)


def cdm_load_daily_reports():
    dwh_pg_connect = ConnectionBuilder.pg_conn("DWH_DB")

    @task(task_id="cdm_daily_reports")
    def daily_datamarts():
        context = get_current_context()
        loader = DailyDatamarts(dwh_pg_connect, log)
        loader.excecute_scripts(cdm_sql_path, context['params'])
    
    run_daily_dm = daily_datamarts()
    run_daily_dm


daily_reports_dag = cdm_load_daily_reports()