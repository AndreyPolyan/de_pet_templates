import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from lib.schema_init import SchemaDdl
from lib.connection_builder import ConnectionBuilder

log = logging.getLogger(__name__)

### SETTINGS ###

# Connection to DWH
dwh_conn_id = "DWH_DB"

# Folder with migration files (Hardcoded paths for testing)
try:
    init_stg_folder = Variable.get("stg_migrations_path")
except Exception:
    init_stg_folder = "/opt/airflow/dags/_migrations_/sql/stg"

try:
    init_dds_folder = Variable.get("dds_migrations_path")
except Exception:
    init_dds_folder = "/opt/airflow/dags/_migrations_/sql/dds"

try:
    init_cdm_folder = Variable.get("cdm_migrations_path")
except Exception:
    init_cdm_folder = "/opt/airflow/dags/_migrations_/sql/cdm"

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
    connection = ConnectionBuilder.pg_conn(dwh_conn_id)
    ddl = SchemaDdl(connection, log)

    @task(task_id="init_stg_tables")
    def init_stg():
        """Apply migration scripts to the STG schema."""
        ddl.init_schema(init_stg_folder)

    @task(task_id="init_dds_tables")
    def init_dds():
        """Apply migration scripts to the DDS schema."""
        ddl.init_schema(init_dds_folder)

    @task(task_id="init_cdm_tables")
    def init_cdm():
        """Apply migration scripts to the CDM schema."""
        ddl.init_schema(init_cdm_folder)

    # Define task execution order
    
    init_stg()  >> init_dds() >> init_cdm()


# Instantiate the DAG
dag_conn = dwh_migrations_init()