import os
import sys
from typing import Union


import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.window import Window

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


def main():
    """
    Entry point for the Spark job. Reads inputs, processes data, and writes outputs.

    Args:
        sys.argv[1] (str): Path to the events data.
        sys.argv[2] (str): Path to the cities data.
        sys.argv[3] (str): Path to the output directory.

    Returns:
        None
    """
    events_path = sys.argv[1]
    cities_data_path = sys.argv[2]
    output_path = sys.argv[3]

    conf = SparkConf().setAppName("dmart_reccomendations")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities_df = get_cities(cities_data_path, sql)
    all_events_df = get_events(events_path, sql, sample_rate=0.1)
    act_loc_df = get_last_location(all_events_df)
    sub_pairs_df = get_user_sub_pairs(all_events_df)
    current_conn_df = get_current_connections(all_events_df)
    close_pairs_df = get_close_pairs(sub_pairs_df, current_conn_df, act_loc_df)
    dmart_df = get_dmart_reccomendations(close_pairs_df, cities_df)

    writer(dmart_df, output_path)




def get_cities(cities_data_path: str, sql: Union[SparkSession, SQLContext]) -> DataFrame:
    """
    Reads city data and preprocesses it by converting coordinates to radians.

    Args:
        cities_data_path (str): Path to the cities data.
        sql (SparkSession | SQLContext): Spark session or SQL context.

    Returns:
        DataFrame: Processed cities DataFrame.
    """
    cities = (sql.read.option("header", True)
              .option("delimiter", ";")
              .csv(cities_data_path)
              .withColumn('lat_number', F.regexp_replace('lat', ',', '.').cast('float'))
              .withColumn('lng_number', F.regexp_replace('lng', ',', '.').cast('float'))
              .withColumn('lat_n_rad', F.radians('lat_number'))
              .withColumn('lng_n_rad', F.radians('lng_number'))
              .drop("lat", "lng", "lat_number", "lng_number")
              .withColumnRenamed('id', 'city_id')
              .persist(StorageLevel.MEMORY_ONLY))
    return cities

def get_events(
    events_path: str,
    sql: Union[SparkSession, SQLContext],
    use_date_filter: bool = False,
    start_date: str = "",
    end_date: str = "",
    sample_rate: float = -1
) -> DataFrame:
    """
    Reads and preprocesses events data, including optional sampling and filtering.

    Args:
        events_path (str): Path to the events data.
        sql (SparkSession | SQLContext): Spark session or SQL context.
        use_date_filter (bool): Whether to apply a date filter.
        start_date (str): Start date for filtering.
        end_date (str): End date for filtering.
        sample_rate (float): Fraction of data to sample.

    Returns:
        DataFrame: Preprocessed events DataFrame.
    """
    events = sql.read.parquet(events_path)

    if use_date_filter and start_date and end_date:
        events = events.where(f'date >= "{start_date}" and date <= "{end_date}"')

    events = (events
              .withColumn("evt_lat_rad", F.radians('lat'))
              .withColumn("evt_lng_rad", F.radians('lon'))
              .drop('lon', 'lat')
              .withColumn(
                  'user_id',
                  F.when(F.col('event_type') == 'subscription', F.col('event.user'))
                   .when(F.col('event_type') == 'reaction', F.col('event.reaction_from'))
                   .when(F.col('event_type') == 'message', F.col('event.message_from'))
              )
              .selectExpr('user_id', 'date', 'event.datetime as evt_datetime', 'event_type',
                          'evt_lat_rad', 'evt_lng_rad', 'event.subscription_channel',
                          'event.message_from', 'event.message_to'))

    if sample_rate > 0:
        events = events.sample(sample_rate)

    events.persist(StorageLevel.MEMORY_ONLY)
    return events

def get_last_location(events: DataFrame) -> DataFrame:
    """
    Extracts the last known location for each user.

    Args:
        events (DataFrame): Preprocessed events DataFrame.

    Returns:
        DataFrame: User location DataFrame with latitude and longitude in radians.
    """
    w = Window.partitionBy('user_id').orderBy(F.desc('evt_datetime'))

    last_location = events.filter(
        F.col('evt_lat_rad').isNotNull() & F.col('evt_lng_rad').isNotNull()
    )

    last_location = (last_location
                     .withColumn("row_number", F.row_number().over(w))
                     .filter(F.col("row_number") == 1)
                     .selectExpr('user_id', 'evt_lat_rad as actual_lat_rad', 'evt_lng_rad as actual_lng_rad')
                     .persist(StorageLevel.MEMORY_ONLY))
    return last_location

def get_user_sub_pairs(events: DataFrame) -> DataFrame:
    """
    Finds pairs of users subscribed to the same channel.

    Args:
        events (DataFrame): Preprocessed events DataFrame.

    Returns:
        DataFrame: DataFrame with user pairs (`user_left`, `user_right`).
    """
    result = (events
              .filter(F.col('event_type') == 'subscription')
              .filter(~F.isnull('subscription_channel'))
              .filter(~F.isnull('user_id'))
              .selectExpr('user_id', 'subscription_channel')
              .repartition(200, 'subscription_channel')
              .persist(StorageLevel.MEMORY_ONLY))

    result = (result
              .withColumnRenamed('user_id', 'user_left')
              .join(
                  result.withColumnRenamed('user_id', 'user_right'),
                  'subscription_channel',
                  'inner'
              )
              .filter(F.col('user_left') < F.col('user_right'))
              .drop('subscription_channel')
              .distinct()
              .repartition(200, 'user_left')
              .persist(StorageLevel.MEMORY_ONLY))
    return result


def get_current_connections(events: DataFrame) -> DataFrame:
    """
    Finds current connections between users based on messages.

    Args:
        events (DataFrame): Preprocessed events DataFrame.

    Returns:
        DataFrame: DataFrame with current user connections.
    """
    result = (events
              .filter(F.col('event_type') == 'message')
              .filter(F.col('message_from').isNotNull() & F.col('message_to').isNotNull())
              .withColumn('user_left', F.least(F.col('message_from'), F.col('message_to')))
              .withColumn('user_right', F.greatest(F.col('message_from'), F.col('message_to')))
              .select('user_left', 'user_right')
              .distinct()
              .repartition(200, 'user_left')
              .persist(StorageLevel.MEMORY_ONLY))
    return result


def get_close_pairs(sub_pairs: DataFrame, contact_pairs: DataFrame, actual_loc: DataFrame, dist: int = 1) -> DataFrame:
    """
    Identifies close pairs of users based on proximity and connections.

    Args:
        sub_pairs (DataFrame): User subscription pairs.
        contact_pairs (DataFrame): User contact pairs.
        actual_loc (DataFrame): User location data.
        dist (int): Distance theshold in km (default: 1)

    Returns:
        DataFrame: DataFrame with user pairs and their midpoint locations.
    """
    result = sub_pairs.alias('a')\
                .join(contact_pairs.alias('b')
                , (F.col('a.user_left') == F.col('b.user_left')) & 
                (F.col('a.user_right') == F.col('b.user_right')),
                'leftanti')\
                .alias('base')\
                .join(actual_loc.alias('l_loc')
                ,F.col('base.user_left') == F.col('l_loc.user_id'),
                'left')\
                .drop('user_id')\
                .withColumnRenamed('actual_lat_rad', 'l_actual_lat_rad')\
                .withColumnRenamed('actual_lng_rad', 'l_actual_lng_rad')\
                .join(actual_loc.alias('r_loc')
                ,F.col('base.user_right') == F.col('r_loc.user_id'),
                'left')\
                .drop('user_id')\
                .withColumnRenamed('actual_lat_rad', 'r_actual_lat_rad')\
                .withColumnRenamed('actual_lng_rad', 'r_actual_lng_rad')\
                .filter((F.col('l_actual_lat_rad').isNotNull()) 
                        & (F.col('l_actual_lng_rad').isNotNull())
                        & (F.col('r_actual_lat_rad').isNotNull())
                        & (F.col('r_actual_lng_rad').isNotNull()))\
                .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
                        F.sqrt(
                            F.pow(F.sin((F.col('l_actual_lat_rad') - F.col('r_actual_lat_rad'))/F.lit(2)),2)
                            + F.cos(F.col("l_actual_lat_rad"))*F.cos(F.col("r_actual_lat_rad"))*
                            F.pow(F.sin((F.col('l_actual_lng_rad') - F.col('r_actual_lng_rad'))/F.lit(2)),2)
                        )))\
                .filter(F.col('distance') < 1)\
                .withColumn(
                    "middle_lat_rad",
                    (F.col("l_actual_lat_rad") + F.col("r_actual_lat_rad")) / 2)\
                .withColumn(
                    "middle_lng_rad",
                    (F.col("l_actual_lng_rad") + F.col("r_actual_lng_rad")) / 2)\
                .drop('l_actual_lat_rad', 'l_actual_lng_rad', 'r_actual_lat_rad', 'r_actual_lng_rad')\
                .persist(StorageLevel.MEMORY_ONLY)
    return result

def get_dmart_reccomendations(pairs_df: DataFrame, cities: DataFrame) -> DataFrame:
    """
    Generates recommendations for user pairs based on the closest city.

    Args:
        pairs_df (DataFrame): DataFrame with user pairs and their midpoints.
        cities (DataFrame): DataFrame with city information.

    Returns:
        DataFrame: DataFrame with user pairs and recommended cities.
    """
    result = (
        pairs_df
        .crossJoin(F.broadcast(cities))
        .withColumn("dist_to_city", F.lit(2) * F.lit(6371) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.col('middle_lat_rad') - F.col('lat_n_rad')) / 2), 2) +
                F.cos(F.col("lat_n_rad")) * F.cos(F.col("middle_lat_rad")) *
                F.pow(F.sin((F.col('middle_lng_rad') - F.col('lng_n_rad')) / 2), 2)
            )
        ))
        .drop("middle_lat_rad", "middle_lng_rad", "lat_n_rad", "lng_n_rad")
    )

    # Define a window to get the closest city for each user pair
    w = Window.partitionBy(['user_left', 'user_right']).orderBy(F.asc_nulls_last('dist_to_city'))

    # Filter the closest city and clean up the result
    result = (
        result
        .withColumn("row_number", F.row_number().over(w))
        .filter(F.col("row_number") == 1)
        .withColumn(
            "city",
            F.when(F.col("dist_to_city").isNotNull(), F.col("city")).otherwise(F.lit(None))
        )
        .withColumn(
            "city_id",
            F.when(F.col("dist_to_city").isNotNull(), F.col("city_id")).otherwise(F.lit(None))
        )
        .drop("dist_to_city", "row_number")
        .filter(F.col('city_id').isNotNull())
        .withColumn('processed_dttm', F.current_timestamp())
        .select('user_left', 'user_right', 'processed_dttm', 'city_id', 'city')
    )
    return result


def writer(df: DataFrame, output_path: str) -> None:
    """
    Writes the DataFrame to the specified output path in Parquet format.

    Args:
        df (DataFrame): DataFrame to write.
        output_path (str): Path to the output directory.

    Returns:
        None
    """
    df.write.mode('overwrite').parquet(output_path)