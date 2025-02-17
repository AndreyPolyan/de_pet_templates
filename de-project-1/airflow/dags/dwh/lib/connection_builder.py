from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable

from .pg_connect import PgConnect
from .mongo_connect import MongoConnect


class ConnectionBuilder:

    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password),
                       sslmode)

        return pg
    
    @staticmethod 
    def mongo_conn(conn_id: str) -> MongoConnect:
        conn = BaseHook.get_connection(conn_id)

        mg  = MongoConnect(user = conn.login,
                           pw = conn.password,
                           host = f'{conn.host}:{conn.port}',
                           main_db = conn.schema)
        return mg
