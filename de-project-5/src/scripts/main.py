import time
from typing import Optional
import yaml 


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType, LongType

# Load configurations from a YAML file for Kafka and PostgreSQL settings
with open("credentials.yaml", "r") as file:
    config = yaml.safe_load(file)

# PostgreSQL read and write configurations

postgresql_read_settings = {
    'driver': 'org.postgresql.Driver',
    'url' : config['SubscriptionSource']['URL'],
    'user' :  config['SubscriptionSource']['USER'],
    'password': config['SubscriptionSource']['PASSWORD'],
    'dbtable': config['SubscriptionSource']['DBTABLE'],
}

postgresql_write_settings = {
    'driver': 'org.postgresql.Driver',
    'url' : config['AnalyticsTarget']['URL'],
    'user' :  config['AnalyticsTarget']['USER'],
    'password': config['AnalyticsTarget']['PASSWORD'],
    'dbtable': config['AnalyticsTarget']['DBTABLE'],
}

# Kafka security options for reading and writing
kafka_read_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.bootstrap.servers': config['KafkaSourceRead']['BOOTSTRAP_SERVERS'],
    'subscribe': config['KafkaSourceRead']['TOPIC'],
    'kafka.sasl.jaas.config': (
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{config["KafkaSourceRead"]["SASL_USERNAME"]}" '
        f'password="{config["KafkaSourceRead"]["SASL_PASSWORD"]}";'
    ),
}

kafka_write_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.bootstrap.servers': config['KafkaTargetWrite']['BOOTSTRAP_SERVERS'],
    'topic': config['KafkaTargetWrite']['TOPIC'],
    'kafka.sasl.jaas.config': (
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{config["KafkaTargetWrite"]["SASL_USERNAME"]}" '
        f'password="{config["KafkaTargetWrite"]["SASL_PASSWORD"]}";'
    ),
}


def spark_init(session_name) -> SparkSession:
    """
    Initialize a SparkSession with specific configurations.

    Args:
        session_name (str): Name of the Spark application.

    Returns:
        SparkSession: Configured SparkSession instance.
    """

    spark_jars_packages = ",".join(
        [
            "org.postgresql:postgresql:42.4.0", # PostgreSQL JDBC driver
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", # Kafka integration for Spark
        ]
        )
    return (SparkSession.builder
            .master("local")
            .appName(session_name)
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
            )  

def read_restaurant_stream(spark: SparkSession) -> Optional[DataFrame]:
    """
    Reads a Kafka stream of restaurant advertisement campaigns.

    Args:
        spark (SparkSession): The active SparkSession.

    Returns:
        Optional[DataFrame]: A streaming DataFrame with campaign data.
    """
    schema = StructType([
        StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", LongType(), True),
        StructField("adv_campaign_datetime_end", LongType()), True,
        StructField("datetime_created", LongType(), True),
    ])

    # Read the Kafka stream, parse JSON data, and add a timestamp column
    df = (spark.readStream.format('kafka')
          .options(**kafka_read_security_options)
          .load()
          .withColumn('value', f.col('value').cast(StringType()))
          .withColumn('event', f.from_json(f.col('value'), schema))
          .selectExpr('event.*', 'offset')
          .withColumn('timestamp',
                      f.from_unixtime(f.col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
          )

    return df

def read_subs(spark: SparkSession) -> DataFrame:
    """
    Reads subscription data from a PostgreSQL database.

    Args:
        spark (SparkSession): The active SparkSession.

    Returns:
        DataFrame: A DataFrame with unique client and restaurant subscriptions.
    """
    return (spark.read
            .format('jdbc') 
            .options(**postgresql_read_settings)
            .load()
            .dropDuplicates(['client_id','restaurant_id'])
            .select(['client_id','restaurant_id'])
        )

def join_transform(restaurant_stream_df, client_subs) -> DataFrame:
    """
    Joins the restaurant stream with client subscriptions and filters campaigns
    active at the current timestamp.

    Args:
        restaurant_stream_df (DataFrame): Streaming DataFrame with restaurant campaigns.
        client_subs (DataFrame): DataFrame with client subscriptions.

    Returns:
        DataFrame: Transformed DataFrame with joined and filtered data.
    Output keys:
    {
        "restaurant_id":"123e4567-e89b-12d3-a456-426614174000",
        "adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003",
        "adv_campaign_content":"first campaign",
        "adv_campaign_owner":"Ivanov Ivan Ivanovich",
        "adv_campaign_owner_contact":"iiivanov@restaurant.ru",
        "adv_campaign_datetime_start":1659203516,
        "adv_campaign_datetime_end":2659207116,
        "client_id":"023e4567-e89b-12d3-a456-426614174000",
        "datetime_created":1659131516,
        "trigger_datetime_created":1659304828
    }
    """
    
    result = (
        restaurant_stream_df
        .withColumn('trigger_datetime_created', f.unix_timestamp(f.current_timestamp()))
        .where(
            (f.col("adv_campaign_datetime_start") < f.col("trigger_datetime_created")) & 
            (f.col("adv_campaign_datetime_end") > f.col("trigger_datetime_created"))
        )
        .dropDuplicates(['restaurant_id', 'adv_campaign_id']) # Handle late data
        .withWatermark('timestamp', '10 minutes')
        .join(client_subs, "restaurant_id", how="inner") 
        .select("restaurant_id", 
                "adv_campaign_id", 
                "adv_campaign_content", 
                "adv_campaign_owner",
                "adv_campaign_owner_contact", 
                "adv_campaign_datetime_start", 
                "adv_campaign_datetime_end",
                "client_id", 
                "datetime_created", 
                "trigger_datetime_created")
            )
    return result

def send_to_kafka(df: DataFrame) -> None: 
    """
    Sends transformed data to a Kafka topic.

    Args:
        df (DataFrame): DataFrame containing data to send to Kafka.
    """
    return (df
            .write
            .format("kafka")
            .options(**kafka_write_security_options)
            .option("checkpointLocation", "test_query")
            .save()
           )
    
def send_to_analytics_db(df: DataFrame) -> None: 
    """
    Writes the DataFrame to an analytics PostgreSQL database.

    Args:
        df (DataFrame): DataFrame to write to the database.
    """
    return (
    df.write \
        .mode("append") \
        .format("jdbc") \
        .options(**postgresql_write_settings)
        .save()
    )

def write_batch(df:DataFrame, epoch_id: int) -> None:
    """
    Processes each micro-batch of data for both Kafka and PostgreSQL outputs.

    Args:
        df (DataFrame): The DataFrame of the current batch.
        epoch_id (int): The batch epoch ID.
    """
    
    #Store the DF so that no need to recalc before sending to multiple targets
    df.persist()
    
    # Prepare data for PostgreSQL
    postgres_df = df.withColumn("feedback", f.lit(None).cast(StringType()))
    send_to_analytics_db(postgres_df)
    
    # Prepare data for Kafka
    kafka_df = (
        df.withColumn("value", 
                          f.to_json(
                              f.struct(
                                f.col('restaurant_id'),
                                f.col('adv_campaign_id'),
                                f.col('adv_campaign_content'),
                                f.col('adv_campaign_owner'),
                                f.col('adv_campaign_owner_contact'),
                                f.col('adv_campaign_datetime_start'),
                                f.col('adv_campaign_datetime_end'),
                                f.col('client_id'),
                                f.col('datetime_created'),
                                f.col('trigger_datetime_created')
                              )
                          )
                     )
    )
        
    send_to_kafka(kafka_df)
    
    #Clear the memory from the batch
    df.unpersist()

def run_query(df):
    """
    Runs the streaming query with batch processing logic.

    Args:
        df (DataFrame): Transformed streaming DataFrame.
    """
    return (df
            .writeStream
            .foreachBatch(write_batch)
            .trigger(processingTime="1 minute")
            .start())

if __name__ == '__main__':
    # Initialize Spark and process the data stream
    spark = spark_init('RestaurantSubscribeStreamingService')
    restaurant_stream = read_restaurant_stream(spark)
    restaurant_subs = read_subs(spark)
    out_data = join_transform(restaurant_stream, restaurant_subs)
    query = run_query(out_data)

    while query.isActive:
        print(
            f"query information: runId={query.runId}, "
            f"status is {query.status}, "
            f"recent progress={query.recentProgress}"
        )
        time.sleep(30)

    query.awaitTermination()