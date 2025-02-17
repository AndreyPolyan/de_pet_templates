from airflow.decorators import dag, task
import logging
import pendulum

from airflow.operators.python import get_current_context

from utils.synthetic_data import generate_order,generate_restaurant, generate_user, insert_data_to_mongo, insert_order
from lib.connection_builder import ConnectionBuilder


log = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'de_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

@dag(
    default_args=default_args,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 9, 1, tz="UTC"), 
    catchup=False,
    tags=['mongo','bonussystem','data_creation', 'dwh'],
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        'new_users_cnt': 10,
        'new_restaurants_cnt': 1,
        'new_orders_cnt':100
    }
)
def load_synthetic_data_mongo():
    pg_conn = ConnectionBuilder.pg_conn('BONUSYSTEM_DB')
    mg_conn = ConnectionBuilder.mongo_conn("MONGO_DB")
    mg_client = mg_conn.client()

    @task(task_id="load_users_mongo")
    def load_synthetic_users():
        user_cnt = get_current_context()['params']['new_users_cnt']
        insert_data_to_mongo(mg_client, 'users', generate_user, ['login'], count= user_cnt)
        log.info(f'Succesfully loaded {user_cnt} objects in MongoDB Users')
    load_users = load_synthetic_users()

    @task(task_id="load_restaurants_mongo")
    def load_synthetic_restaurants():
        rest_cnt = get_current_context()['params']['new_restaurants_cnt']
        insert_data_to_mongo(mg_client, 'restaurants', generate_restaurant, ['name'], count= rest_cnt)
        log.info(f'Succesfully loaded {rest_cnt} objects in MongoDB Restaurants')
    load_restaurants = load_synthetic_restaurants()

    @task(task_id="load_orders_mongo")
    def load_synthetic_orders():
        orders_cnt = get_current_context()['params']['new_orders_cnt']
        users = list(mg_client.users.find())
        rests = list(mg_client.restaurants.find())
        insert_order(mg_client = mg_client, collection_name= 'orders',pg_client= pg_conn, data_generator = generate_order, count= orders_cnt, existing_restaurants = rests, existing_users = users)
        log.info(f'Succesfully loaded {orders_cnt} objects in MongoDB Orders')
    load_orders = load_synthetic_orders()

    [load_users,load_restaurants] >> load_orders

load_mongo_data = load_synthetic_data_mongo()