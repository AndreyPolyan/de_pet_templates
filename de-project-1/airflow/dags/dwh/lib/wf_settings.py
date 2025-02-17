from typing import Dict, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel



class EtlSetting(BaseModel):
    """Represents an ETL workflow setting."""

    id: int
    workflow_key: str
    workflow_settings: Dict



class EtlSettingsRepository:
    """Repository for managing ETL workflow settings in DWH."""

    def __init__(self, schema: str) -> None:
        """
        Initialize the repository with the target schema.

        Args:
            schema (str): Database schema name.
        """
        self.schema = schema
    
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        """
        Retrieve an ETL workflow setting from the database.

        Args:
            conn (psycopg.Connection): Database connection instance.
            etl_key (str): The workflow key to fetch settings for.

        Returns:
            Optional[EtlSetting]: Retrieved ETL setting or None if not found.
        """
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                f"""
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM {self.schema}.srv_wf_settings
                    WHERE workflow_key = %s;
                """,
                (etl_key,)
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        """
        Save or update an ETL workflow setting.

        Args:
            conn (psycopg.Connection): Database connection instance.
            etl_key (str): The workflow key to store settings for.
            workflow_settings (dict): The workflow settings data.

        Raises:
            Exception: If the database update fails.
        """
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO {self.schema}.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%s, %s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                (workflow_key,workflow_settings,),
            )