from typing import Dict, Optional
from vertica_connect import VerticaConnect
from utils.dict_util import json2str, str2json
from pydantic import BaseModel


class EtlSetting(BaseModel):
    """Represents an ETL workflow setting."""

    id: int
    workflow_key: str
    workflow_settings: Dict


def map_rows_to_model(rows, model):
    """
    Map database query results to a Pydantic model.

    Args:
        rows (list): List of database query results.
        model (BaseModel): Pydantic model to map rows to.

    Returns:
        list: List of mapped model instances.
    """
    return [
        model(
            **{
                "id": row[0],
                "workflow_key": row[1],
                "workflow_settings": str2json(row[2]),
            }
        )
        for row in rows
    ]


class EtlSettingsRepository:
    """Repository for managing ETL workflow settings in Vertica."""

    def __init__(self, schema: str) -> None:
        """
        Initialize the repository with the target schema.

        Args:
            schema (str): Database schema name.
        """
        self.schema = schema

    def get_setting(self, conn: VerticaConnect, etl_key: str) -> Optional[EtlSetting]:
        """
        Retrieve an ETL workflow setting from the database.

        Args:
            conn (VerticaConnect): Database connection instance.
            etl_key (str): The workflow key to fetch settings for.

        Returns:
            Optional[EtlSetting]: Retrieved ETL setting or None if not found.
        """
        query = f"""
            SELECT
                id,
                workflow_key, 
                workflow_settings
            FROM {self.schema}.wf_settings
            WHERE workflow_key = '{etl_key}'
            LIMIT 1;
        """

        with conn.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()
                return map_rows_to_model(result, EtlSetting)[0] if result else None

    def save_setting(
        self, conn: VerticaConnect, etl_key: str, workflow_settings: Dict
    ) -> None:
        """
        Save or update an ETL workflow setting.

        Args:
            conn (VerticaConnect): Database connection instance.
            etl_key (str): The workflow key to store settings for.
            workflow_settings (dict): The workflow settings data.

        Raises:
            Exception: If the database update fails.
        """
        formatted_wf = json2str(workflow_settings) if workflow_settings else ""

        upsert_query = f"""
            DELETE FROM {self.schema}.wf_settings WHERE workflow_key = :workflow_key;
            INSERT INTO {self.schema}.wf_settings (workflow_key, workflow_settings) 
            VALUES (:workflow_key, :workflow_settings);
        """

        with conn.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    upsert_query,
                    {"workflow_key": etl_key, "workflow_settings": formatted_wf},
                )