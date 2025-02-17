import logging
import pendulum

from lib import ConnectionBuilder

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from analytics.stg.loaders.bonussystem import EventLoader as BsStgEventLoader
from lib.dag_support.dwh_entities_generator import DwhEntitiesDag

from analytics.dds.loaders import FctSaleLoader as DdsFctSaleLoader,\
                            OrderLoader as DdsOrderLoader,ProductLoader as DdsProductLoader,\
                            TimeLoader as DdsTimeLoader, UserLoader as DdsUserLoader, RestaurantLoader as DdsRestaurantLoader

default_args = {
    'owner': 'de_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

log = logging.getLogger(__name__)

#Task generators
bs_stg_task_gen = DwhEntitiesDag.stg_bs_create_task
os_stg_task_gen = DwhEntitiesDag.stg_os_create_task
dds_task_gen = DwhEntitiesDag.dds_create_task



@dag(
    schedule_interval='0/15 * * * *', 
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False,  
    tags=['dwh', 'dds', 'stg', 'origin'],  
    is_paused_upon_creation=True,  
    max_active_runs=1
)
def dwh_load_stg_dds_entities():
    dwh_pg_connect = ConnectionBuilder.pg_conn("DWH_DB")
    origin_pg_connect = ConnectionBuilder.pg_conn("BONUSYSTEM_DB")
    origin_mg_connect = ConnectionBuilder.mongo_conn("MONGO_DB")

    with TaskGroup(group_id='stg_layer') as stg:
        with TaskGroup(group_id = 'bonussystem_stg') as stg_bs:
            stg_load_events_bs_task = bs_stg_task_gen(BsStgEventLoader, "bs_event_load", origin_pg_connect, dwh_pg_connect,log)()
            
            stg_load_events_bs_task
        
        with TaskGroup(group_id='ordersystem_stg') as stg_os:
            stg_load_restaurants_os_task = os_stg_task_gen(task_id= 'ordsys_load_restaurants',
                                                           wf_key= 'ordersystem_restaurants_origin_to_stg_workflow',
                                                           collection_name= 'restaurants',
                                                           dwh_table_name= 'ordersystem_restaurants',
                                                           mg_conn= origin_mg_connect,
                                                           dwh_connect= dwh_pg_connect,
                                                           log= log)()
            stg_load_users_os_task = os_stg_task_gen(task_id= 'ordsys_load_users',
                                                           wf_key= 'ordersystem_users_origin_to_stg_workflow',
                                                           collection_name= 'users',
                                                           dwh_table_name= 'ordersystem_users',
                                                           mg_conn= origin_mg_connect,
                                                           dwh_connect= dwh_pg_connect,
                                                           log= log)()
            stg_load_orders_os_task = os_stg_task_gen(task_id= 'ordsys_load_orders',
                                                           wf_key= 'ordersystem_orders_origin_to_stg_workflow',
                                                           collection_name= 'orders',
                                                           dwh_table_name= 'ordersystem_orders',
                                                           mg_conn= origin_mg_connect,
                                                           dwh_connect= dwh_pg_connect,
                                                           log= log)()
            [stg_load_restaurants_os_task, stg_load_users_os_task, stg_load_orders_os_task]

        [stg_bs, stg_os]

    with TaskGroup(group_id='dds_layer') as dds:
        with TaskGroup(group_id='dds_1st_level') as dds_level_1:
            dds_load_user_task = dds_task_gen(DdsUserLoader, 'load_users_from_stg', dwh_pg_connect, log)()
            dds_load_restaurants_task = dds_task_gen(DdsRestaurantLoader, 'load_restaurants_from_stg', dwh_pg_connect, log)()
            dds_load_time_task = dds_task_gen(DdsTimeLoader, 'load_time_from_stg', dwh_pg_connect, log)()

            [dds_load_user_task, dds_load_restaurants_task, dds_load_time_task]

        with TaskGroup(group_id='dds_2nd_level') as dds_level_2:
            dds_load_products_task = dds_task_gen(DdsProductLoader, 'load_products_from_stg', dwh_pg_connect, log)()
            dds_load_orders_task = dds_task_gen(DdsOrderLoader, 'load_orders_from_stg', dwh_pg_connect, log)()

            [dds_load_products_task, dds_load_orders_task]
        
        with TaskGroup(group_id='dds_3rd_level') as dds_level_3:
            dds_load_fctdelivery_task = dds_task_gen(DdsFctSaleLoader, 'load_fact_sales', dwh_pg_connect, log)()
            [dds_load_fctdelivery_task]

        dds_level_1 >> dds_level_2 >> dds_level_3
    stg >> dds

dwh_load_entities_dag = dwh_load_stg_dds_entities()