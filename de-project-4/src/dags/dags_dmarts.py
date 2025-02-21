from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

# Environment setup
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

@dag(
    dag_id="datalake_dmarts",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="DAG for creating data marts using Spark jobs"
)
def datalake_dmarts():

    # Task 1: dmart_users
    dmart_users = SparkSubmitOperator(
        task_id='dmart_users',
        application='/lessons/dmart_users.py',
        conn_id='yarn_spark',
        application_args=[
            "/user/master/data/geo", 
            "/user/andreypoly/data/analytics/project7/cities", 
            "/user/andreypoly/data/analytics/project7/dmart_users"
        ],
        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.driver.memory": "2g",
            "spark.driver.cores": "2",
            "spark.master": "yarn",
            "spark.executor.instances": "4",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2"
        }
    )

    # Task 2: dmart_zones
    dmart_zones = SparkSubmitOperator(
        task_id='dmart_zones',
        application='/lessons/dmart_zones.py',
        conn_id='yarn_spark',
        application_args=[
            "/user/master/data/geo", 
            "/user/andreypoly/data/analytics/project7/cities",
            "/user/andreypoly/data/analytics/project7/dmart_zones"
        ],
        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.driver.memory": "2g",
            "spark.driver.cores": "2",
            "spark.master": "yarn",
            "spark.executor.instances": "4",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2"
        }
    )

    # Task 3: dmart_reccomendations
    dmart_reccomendations = SparkSubmitOperator(
        task_id='dmart_reccomendations',
        application='/lessons/dmart_reccomendations.py',
        conn_id='yarn_spark',
        application_args=[
            "/user/master/data/geo", 
            "/user/andreypoly/data/analytics/project7/cities", 
            "/user/andreypoly/data/analytics/project7/dmart_reccomendations"
        ],
        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.driver.memory": "2g",
            "spark.driver.cores": "2",
            "spark.master": "yarn",
            "spark.executor.instances": "8",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "8",
            "spark.dynamicAllocation.maxExecutors": "16",
            "spark.dynamicAllocation.initialExecutors": "8",
            "spark.executor.heartbeatInterval": "10s",
            "spark.dynamicAllocation.executorIdleTimeout": "60s",
            "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
            "spark.shuffle.service.enabled": "true"
        }
    )

    # Define task dependencies
    dmart_users >> dmart_zones >> dmart_reccomendations

# Instantiate the DAG
datalake_dmarts = datalake_dmarts()