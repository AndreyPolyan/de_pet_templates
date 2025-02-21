from airflow.decorators import dag, task

from lib.schema_init import SchemaDdl
from analytics.stg.loaders.ordersystem import MgCollectionLoader, MgCollectionReader
from lib import MongoConnect, PgConnect, PgSaver
from logging import Logger
#Блок с вспомогательными функциями для сокращения тасок в DAG

class DwhEntitiesDag:
    @staticmethod
    def stg_bs_create_task(loader_class, task_id, origin_connect, dwh_connect, log):
            @task(task_id=task_id)
            def loader_task():
                loader = loader_class(origin_connect, dwh_connect, log)
                loader.load_data()
            return loader_task
    @staticmethod
    def stg_os_create_task(task_id: str,
                            wf_key: str,
                            collection_name: str,
                            dwh_table_name: str,
                            mg_conn: MongoConnect,
                            dwh_connect: PgConnect,
                            log: Logger):
            @task(task_id=task_id)
            def loader_task():
                pg_saver = PgSaver(table_name = dwh_table_name)
                collection_reader = MgCollectionReader(mg_conn)
                loader = MgCollectionLoader(collection_reader=collection_reader,
                                            pg_dest=dwh_connect,
                                            pg_saver=pg_saver,
                                            logger=log,
                                            collection_name=collection_name,
                                            WF_KEY=wf_key)

                loader.load_data()
            return loader_task
    @staticmethod
    def dds_create_task(loader_class, task_id,dwh_connect,log):
        @task(task_id=task_id)
        def loader_task():
            loader = loader_class(dwh_connect, log)
            loader.load_data()
        return loader_task
    @staticmethod
    def init_sql_task(task_id,sql_path, dwh_connect,log):
        @task(task_id=task_id)
        def loader_task():
            loader = SchemaDdl(dwh_connect, log)
            loader.init_schema(sql_path)
        return loader_task