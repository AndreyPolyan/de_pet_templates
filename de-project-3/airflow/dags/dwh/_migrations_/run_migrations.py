import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from lib.schema_init import SchemaDdl
from lib.connection_builder import ConnectionBuilder

log = logging.getLogger(__name__)

### SETTINGS ###

# Connection to DWH
vertica_conn_id = "VERTICA_CONN"

# Folder with migration files (Hardcoded paths for testing)
try:
    init_stg_folder = Variable.get("stg_migrations_path")
except Exception:
    init_stg_folder = "/opt/airflow/dags/_migrations_/sql/stg"


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
    schedule="*/10 * * * *",  # Every 10 minutes
    start_date=pendulum.datetime(2024, 9, 1, tz="UTC"),
    catchup=False,
    tags=["dwh", "migration"],
    max_active_runs=1,
    is_paused_upon_creation=True,
)
def dwh_migrations_init():
    """
    DAG for initializing schema migrations in a Vertica-based DWH.

    This DAG applies migration scripts to STG, DDS, and CDM schemas.
    """
    connection = ConnectionBuilder.vertica_conn(vertica_conn_id)
    ddl = SchemaDdl(connection, log)

    @task(task_id="init_stg_tables")
    def init_stg():
        """Apply migration scripts to the STG schema."""
        ddl.init_schema(init_stg_folder)

    # Define task execution order
    init_stg()


# Instantiate the DAG
dag_conn = dwh_migrations_init()