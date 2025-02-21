import sys
import os
from typing import Union

import pyspark.sql.functions as F 
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.window import Window 
from pyspark.sql.types import  IntegerType

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

    conf = SparkConf().setAppName(f"dmart_zones")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities_df = get_cities(cities_data_path, sql)
    events_df = get_events(events_path, sql)
    events_geo_df = get_events_with_geo(events_df, cities_df)
    events_pop_geo_df = get_events_pop_city(events_geo_df)
    reg_stat = get_registrations_stat(events_pop_geo_df)
    events_stat = get_events_stat(events_pop_geo_df)
    dmart_df = get_dmart_zones(reg_stat, events_stat)
    writer(dmart_df, output_path)

def get_cities(cities_data_path: str, sql: Union[SparkSession, SQLContext]) -> DataFrame:
    """
    Reads city data from local storage and preprocesses it.

    Args:
        cities_data_path (str): Path to the cities data file.
        sql (SparkSession | SQLContext): Spark session or SQL context.

    Returns:
        DataFrame: Processed cities DataFrame with coordinates converted to radians.
    """
    cities = (sql.read.option("header", True)
              .option("delimiter", ";")
              .csv(f'{cities_data_path}')
              .withColumn('lat_number', F.regexp_replace('lat', ',' , '.').cast('float'))  # Convert string to float
              .withColumn('lng_number', F.regexp_replace('lng', ',' , '.').cast('float'))  # Convert string to float
              .withColumn('lat_n_rad', F.radians('lat_number'))  # Convert degrees to radians
              .withColumn('lng_n_rad', F.radians('lng_number'))  # Convert degrees to radians
              .drop("lat", "lng", "lat_number", "lng_number")  # Drop unnecessary columns
              .withColumnRenamed('id', 'city_id') 
              .persist(StorageLevel.MEMORY_ONLY)  # Cache the DataFrame
              )
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
        events_path (str): Path to the events data file.
        sql (SparkSession | SQLContext): Spark session or SQL context.
        use_date_filter (bool): Whether to apply a date filter.
        start_date (str): Start date for filtering (inclusive).
        end_date (str): End date for filtering (inclusive).
        sample_rate (float): Fraction of data to sample.

    Returns:
        DataFrame: Preprocessed events DataFrame.
    """
    # Read the events DataFrame
    events = sql.read.parquet(f'{events_path}')
    
    # Conditionally apply the date filter
    if use_date_filter and start_date and end_date:
        events = events.where(f'date >= "{start_date}" and date <= "{end_date}"')
    
    # Process the events DataFrame
    events = (events
              .withColumn("evt_lat_rad", F.radians('lat'))  # Convert latitude to radians
              .withColumn("evt_lng_rad", F.radians('lon'))  # Convert longitude to radians
              .drop('lon', 'lat')  # Drop original lat/lon columns
              .withColumn(
                  'user_id',
                  F.when(F.col('event_type') == 'subscription', F.col('event.user'))
                   .when(F.col('event_type') == 'reaction', F.col('event.reaction_from'))
                   .when(F.col('event_type') == 'message', F.col('event.message_from'))
              )
              .selectExpr('user_id','date','event.datetime as evt_datetime', 'event_type','evt_lat_rad','evt_lng_rad')
              )
    
    if sample_rate > 0:
        events = events.sample(sample_rate)

    events.persist(StorageLevel.MEMORY_ONLY)
    
    return events

def get_events_with_geo(events: DataFrame, cities: DataFrame) -> DataFrame:
    """
    Matches events to their nearest city based on geographic coordinates.

    Args:
        events (DataFrame): Events DataFrame with geographic coordinates.
        cities (DataFrame): Cities DataFrame with geographic coordinates.

    Returns:
        DataFrame: Events DataFrame augmented with city information.
    """
    # If DF already comes with cities/ids
    events_with_geo = events.drop('city', 'id', 'evt_id', 'city_id') \
                            .withColumn('evt_id', F.monotonically_increasing_id())
    
    # Cross join events with cities and calculate distance
    events_with_geo = (
        events_with_geo
        .crossJoin(F.broadcast(cities))
        .withColumn("dist_to_city", F.lit(2) * F.lit(6371) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.col('evt_lat_rad') - F.col('lat_n_rad')) / F.lit(2)), 2) +
                F.cos(F.col("lat_n_rad")) * F.cos(F.col("evt_lat_rad")) *
                F.pow(F.sin((F.col('evt_lng_rad') - F.col('lng_n_rad')) / F.lit(2)), 2)
            )
        ))
        .drop("evt_lat_rad", "evt_lng_rad", "lat_n_rad", "lng_n_rad")
    )
    
    # Window to assign row numbers based on distance
    w = Window.partitionBy('evt_id').orderBy(F.asc_nulls_last('dist_to_city'))

    # Assign row numbers and retain one row per evt_id
    events_with_geo = (
        events_with_geo
        .withColumn("row_number", F.row_number().over(w))
        .filter(F.col("row_number") == 1)  # Keep the closest city or the first null row
        .withColumn(
            "city",
            F.when(F.col("dist_to_city").isNotNull(), F.col("city")).otherwise(F.lit(None)) #Clear city for lines with no distance
        )
        .withColumn(
            "city_id",
            F.when(F.col("dist_to_city").isNotNull(), F.col("city_id")).otherwise(F.lit(None)) #Clear city for lines with no distance
        )
        .drop("row_number", "timezone", "dist_to_city")
        .persist(StorageLevel.MEMORY_ONLY)
    )
    
    return events_with_geo

def get_events_pop_city(geo_events: DataFrame) -> DataFrame:
    """
    Populates missing city information in the events DataFrame by propagating values
    from the nearest valid events, prioritizing messages.

    Args:
        geo_events (DataFrame): Events DataFrame with geographic information, including
                                missing cities (`city_id` and `city`).

    Returns:
        DataFrame: Events DataFrame with populated city information and an additional
                   column indicating whether a city was added (`added_city`).
    """
    w1 = Window()\
        .partitionBy(['user_id'])\
        .orderBy(F.when(F.col('event_type') == 'message', 1).otherwise(0).asc(), F.asc('evt_datetime'))\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    w2 = Window()\
        .partitionBy(['user_id'])\
        .orderBy(F.when(F.col('event_type') == 'message', 1).otherwise(0).desc(), F.asc('evt_datetime'))\
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    
    events_pop = geo_events\
                    .withColumn('city_id_prev', F.when( F.col('city_id').isNull() , F.last('city_id', ignorenulls = True).over(w1) ).otherwise(F.col('city_id')))\
                    .withColumn('city_prev', F.when( F.col('city').isNull() , F.last('city', ignorenulls = True).over(w1) ).otherwise(F.col('city')))\
                    .withColumn('city_id_next', F.when( F.col('city_id').isNull() , F.first('city_id', ignorenulls = True).over(w2) ).otherwise(F.col('city_id')))\
                    .withColumn('city_next', F.when( F.col('city').isNull() , F.first('city', ignorenulls = True).over(w2) ).otherwise(F.col('city')))\
                    .withColumn('added_city', F.when(F.col('city_id').isNull() & ~F.isnull(F.coalesce(F.col('city_id_prev'), F.col('city_id_next'))), 1).otherwise(0))\
                    .withColumn('city_id', F.when(F.col('city_id').isNull(), F.coalesce(F.col('city_id_prev'), F.col('city_id_next'))).otherwise(F.col('city_id')))\
                    .withColumn('city', F.when(F.col('city').isNull(), F.coalesce(F.col('city_prev'), F.col('city_next'))).otherwise(F.col('city')))\
                    .drop('city_id_prev', 'city_prev','city_id_next','city_next')\
                    .persist(StorageLevel.MEMORY_ONLY)
    
    return events_pop
    
def get_registrations_stat(events: DataFrame) -> DataFrame:
    """
    Computes registration statistics based on the first message event of each user
    in a given city, aggregated by week and month.

    Args:
        events (DataFrame): Events DataFrame containing user, event type, city, and timestamps.

    Returns:
        DataFrame: Registration statistics including the number of weekly and monthly users
                   in each city (zone_id), along with week and month identifiers.
    """
    w1 = Window().partitionBy(['user_id']).orderBy(F.col('evt_datetime').asc())
    w2 = Window().partitionBy('month', 'city_id')
    
    reg_stat = events.filter(
                    (F.col("event_type") == "message") &  # Filter for event_type = "message"
                    (F.col("city_id").isNotNull())       # Filter for non-null city_id
                    )\
                .withColumn('row', F.row_number().over(w1))\
                .filter(F.col('row') == 1)\
                .withColumn("month",F.trunc(F.col("date"), "month"))\
                .withColumn("week",F.trunc(F.col("date"), "week"))\
                .groupBy('month', 'week', 'city_id').agg(F.count('*').alias('week_user'))\
                .withColumn("month_user", F.sum('week_user').over(w2))\
                .withColumnRenamed('city_id', 'zone_id')\
                .persist(StorageLevel.MEMORY_ONLY)
    return reg_stat

def get_events_stat(events: DataFrame) -> DataFrame:
    """
    Computes statistics for messages, reactions, and subscriptions, aggregated
    by week and month for each city (zone_id).

    Args:
        events (DataFrame): Events DataFrame containing city information, event type, and timestamps.

    Returns:
        DataFrame: Event statistics including weekly and monthly counts of messages,
                   reactions, and subscriptions for each city.
    """
    w = Window().partitionBy('month', 'city_id')
    
    reg_stat = events.filter(
                    (F.col("city_id").isNotNull())       # Filter for non-null city_id
                    )\
                .withColumn("month",F.trunc(F.col("date"), "month"))\
                .withColumn("week",F.trunc(F.col("date"), "week"))\
                .groupBy('month', 'week', 'city_id').agg(\
                    F.sum(F.when(F.col('event_type') == 'message', F.lit(1))).alias('week_message'),\
                    F.sum(F.when(F.col('event_type') == 'reaction', F.lit(1))).alias('week_reaction'),\
                    F.sum(F.when(F.col('event_type') == 'subscription', F.lit(1))).alias('week_subscription'))\
                .withColumn("month_message", F.sum('week_message').over(w))\
                .withColumn("month_reaction", F.sum('week_reaction').over(w))\
                .withColumn("month_subscription", F.sum('week_subscription').over(w))\
                .withColumnRenamed('city_id', 'zone_id')\
                .persist(StorageLevel.MEMORY_ONLY)
    return reg_stat

def get_dmart_zones(registrations: DataFrame, events_stat: DataFrame) -> DataFrame:
    """
    Combines registration and event statistics into a unified DataFrame, aggregating
    data by week, month, and zone (city).

    Args:
        registrations (DataFrame): DataFrame containing registration statistics
                                   (weekly and monthly users per zone).
        events_stat (DataFrame): DataFrame containing event statistics
                                 (weekly and monthly counts of messages, reactions, and subscriptions).

    Returns:
        DataFrame: Unified DataFrame with combined registration and event statistics,
                   ordered by month, week, and zone_id.
    """
    result = registrations.join(events_stat, ["month", "week", "zone_id"], 'fullouter')\
                .select('month', 'week', 'zone_id'
                        ,'week_message', 'week_reaction', 'week_subscription', 'week_user'
                        ,'month_message', 'month_reaction', 'month_subscription', 'month_user')\
                .orderBy(F.col('month'), F.col('week'), F.col('zone_id').cast(IntegerType()))\
                .persist(StorageLevel.MEMORY_ONLY)
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
    return df \
        .write \
        .mode('overwrite') \
        .partitionBy('month')\
        .parquet(f'{output_path}')

if __name__ == '__main__':
    main()