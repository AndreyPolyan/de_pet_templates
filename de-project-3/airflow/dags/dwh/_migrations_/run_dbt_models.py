import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

### SETTINGS ###
try:
    dbt_project_path = Variable.get("dbt_project_path")
except Exception:
    dbt_project_path = "/opt/dbt/de_project_3"

try:
    dbt_profile_prod = Variable.get("dbt_profile_prod")
except Exception:
    dbt_profile_prod = "prod"

# Default DAG parameters
default_args = {
    "owner": "de_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
}

### END SETTINGS ###

@dag(
    default_args=default_args,
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=False,
    params={"launch_date": None}, 
    tags=["dwh", "dbt"],
    max_active_runs=1,
    is_paused_upon_creation=True,
)
def dwh_dbt_job():
    """DAG for executing daily ETL tasks in a Vertica-based DWH."""

    @task
    def get_launch_date(**context):
        """Extracts launch_date from DAG params or defaults to yesterday's execution date."""
        dag_run_conf = context["dag_run"].conf if "dag_run" in context else {}
        return dag_run_conf.get("launch_date", (pendulum.parse(context["ds"]) - pendulum.duration(days=1)).to_date_string())

    # Get launch_date dynamically (default: ds - 1 day)
    launch_date = get_launch_date()

    load_dbt_models = BashOperator(
        task_id="load_dbt_models",
        bash_command=(
            f'cd {dbt_project_path} && '
            f'dbt run --target {dbt_profile_prod} -m cdm --vars \'{{"launch_date": "{launch_date}"}}\''
        )
    )

    launch_date >> load_dbt_models


dag_instance = dwh_dbt_job()