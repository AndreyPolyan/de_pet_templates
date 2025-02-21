import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from lib.connection_builder import ConnectionBuilder
from stg.loaders import CurrenciesLoader, TransactionLoader

log = logging.getLogger(__name__)

### SETTINGS ###

# Connection to DWH
vertica_conn_id = "VERTICA_CONN"


# Connection to S3
aws_access_key, aws_secret_key, aws_endpoint_url = (
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "S3_ENDPOINT",
)

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
    schedule="0 * * * *",  # Runs every hour
    start_date=pendulum.datetime(2024, 9, 1, tz="UTC"),
    catchup=False,
    tags=["stg", "dwh", "load"],
    max_active_runs=1,
    is_paused_upon_creation=True,
)
def dwh_load_stg_data():
    """
    DAG for loading data into the staging (STG) layer of the DWH.

    Loads currency and transaction data from an S3 source into the Vertica database.
    """
    connection = ConnectionBuilder.vertica_conn(vertica_conn_id)
    s3_origin = ConnectionBuilder.s3_conn(aws_access_key, aws_secret_key, aws_endpoint_url)

    with TaskGroup(group_id="stg_layer") as stg:
        @task(task_id="load_currencies_to_stg")
        def stg_load_currencies():
            """Load currency data into the STG layer."""
            stg_loader = CurrenciesLoader(s3_origin, connection, log)
            stg_loader.load_data()

        @task(task_id="load_transactions_to_stg")
        def stg_load_transactions():
            """Load transaction data into the STG layer."""
            stg_loader = TransactionLoader(s3_origin, connection, log)
            stg_loader.load_data()

        [stg_load_currencies(),stg_load_transactions()]

    stg


# Instantiate the DAG
dag_conn = dwh_load_stg_data()