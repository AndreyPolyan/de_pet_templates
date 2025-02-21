import sys
import os
from typing import Union

import pyspark.sql.functions as F 
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.window import Window 


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


def main() -> None:
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

    conf = SparkConf().setAppName(f"dmart_users")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities_df = get_cities(cities_data_path, sql)
    events_df = get_events(events_path, sql)
    events_with_geo_df = get_events_with_geo(events_df, cities_df)
    users_actual_geo_df = get_actual_geo(events_with_geo_df)
    users_travel_df = get_travel_geo(events_with_geo_df)
    writer(get_dmart_users(users_actual_geo_df, users_travel_df), output_path)

    

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
            .withColumn('lat_number', F.regexp_replace('lat', ',' , '.').cast('float')) #Convert string to float
            .withColumn('lng_number', F.regexp_replace('lng', ',' , '.').cast('float')) #Convert string to float
            .withColumn('lat_n_rad',F.toRadians('lat_number')) #Convert degrees to radians
            .withColumn('lng_n_rad',F.toRadians('lng_number')) #Convert degrees to radians
            .drop("lat","lng","lat_number","lng_number")
            .persist(StorageLevel.MEMORY_ONLY)
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
                  .where('event_type = "message"') #take only message events
                  .where(F.col('event.message_ts').isNotNull())
                  .select("event.message_id", "event.message_from", "event.message_ts", "lat", "lon") #Columns to work
                  .withColumn("date", F.to_date(F.col("message_ts")))\
                  .withColumn("msg_lat_rad",F.toRadians('lat')) #Coordinates in radian
                  .withColumn('msg_lng_rad',F.toRadians('lon')) #Coordinates in radian
                  .drop("lat","lon") #Drop coordinates in degrees
                  )
    if sample_rate > 0:
        events = events.sample(sample_rate)

    events.persist(StorageLevel.MEMORY_ONLY)

    return events

def get_events_with_geo(events: DataFrame, cities: DataFrame) -> DataFrame:
    """
    Enriches the events DataFrame with the nearest city for each message, based on geographic coordinates.

    Args:
        events (DataFrame): DataFrame containing event data with columns `msg_lat_rad` and `msg_lng_rad`
                            representing the latitude and longitude of messages in radians.
        cities (DataFrame): DataFrame containing city data with columns `lat_n_rad` and `lng_n_rad`
                            representing the latitude and longitude of cities in radians.

    Returns:
        DataFrame: An enriched DataFrame containing:
            - All original columns from the `events` DataFrame.
            - The nearest city's `id` and `name` appended to each row.
            - The distance to the nearest city is calculated but not retained in the final output.

    Raises:
        Exception: If the required columns (`msg_lat_rad`, `msg_lng_rad`, `lat_n_rad`, `lng_n_rad`) are missing.
    """
    events_with_geo = (
        events
        .crossJoin(F.broadcast(cities))
        .withColumn("dist_to_city", F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('msg_lat_rad') - F.col('lat_n_rad'))/F.lit(2)),2)
            + F.cos(F.col("lat_n_rad"))*F.cos(F.col("msg_lat_rad"))*
            F.pow(F.sin((F.col('msg_lng_rad') - F.col('lng_n_rad'))/F.lit(2)),2)
        )))
        .drop("msg_lat_rad","msg_lng_rad","lat_n_rad", "lng_n_rad")
        )
    window = Window()\
                .partitionBy('message_id')\
                .orderBy(F.col('dist_to_city').asc()) #Setting window for every message
    events_with_geo = (
        events_with_geo
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop('row_number')
        .persist(StorageLevel.MEMORY_ONLY)
        )
    return events_with_geo

def get_actual_geo(events_with_geo: DataFrame) -> DataFrame:
    """
    Extracts the latest city information for each user based on the most recent message timestamp.

    Args:
        events_with_geo (DataFrame): A DataFrame containing enriched event data with city information.
                                     Expected columns:
                                     - `message_from`: User identifier.
                                     - `message_ts`: Timestamp of the message.
                                     - `city`: City name associated with the event.
                                     - `id`: City identifier.
                                     - `timezone`: Timezone of the city.

    Returns:
        DataFrame: A DataFrame containing:
            - `user_id`: User identifier.
            - `act_city`: Latest city name associated with the user.
            - `act_city_id`: Latest city identifier.
            - `local_time`: Local timestamp of the user's last activity.

    Raises:
        Exception: If required columns (`message_from`, `message_ts`, `city`, `id`, `timezone`) are missing.
    """
    window = Window().partitionBy('message_from').orderBy(F.col('message_ts').desc())
    act_city = events_with_geo\
            .filter(F.col('city').isNotNull())\
            .withColumn("row_number", F.row_number().over(window))\
            .filter(F.col('row_number')==1)\
            .withColumn("TIME",F.col("message_ts").cast("Timestamp"))\
            .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
            .selectExpr('message_from as user_id', 'city as act_city', 'id as act_city_id','local_time')\
            .persist(StorageLevel.MEMORY_ONLY)
    return act_city

def get_travel_geo(events_with_geo: DataFrame) -> DataFrame:
    """
    Calculates travel statistics for each user, including visited cities, travel chains, and home city.

    The function identifies all city visits by users, determines the duration of stays, and calculates 
    a user's home city if they have stayed in one city continuously for at least 27 days. It also 
    aggregates the travel chain of cities visited by each user in chronological order.

    Args:
        events_with_geo (DataFrame): A DataFrame containing enriched event data with city information.
                                     Expected columns:
                                     - `message_from`: User identifier.
                                     - `message_ts`: Message timestamp.
                                     - `city`: City name associated with the event.
                                     - `id`: City identifier.
                                     - `date`: Date of the event.
    Returns:
        DataFrame: A DataFrame containing:
            - `user_id`: User identifier.
            - `travel_count`: Total number of city stays for the user.
            - `travel_array`: Chronological list of cities visited by the user.
            - `home_city`: Home city of the user, if identified.
            - `home_city_id`: Identifier of the home city.

    Raises:
        Exception: If required columns (`message_from`, `message_ts`, `city`, `id`, `date`) are missing.

    Notes:
        - The home city is identified as the city where a user stayed for at least 27 consecutive days.
        - Travel chains include all visited cities, including repeated visits.
        - Intermediate calculations include stay start, end, duration, and flags for long stays.
        - The result is persisted in memory for optimization during subsequent operations.
    """
    #Step 1 - mark all messages with stay start and end
    w1 = Window().partitionBy('message_from').orderBy(F.col('message_ts').asc())
    
    travel_df = events_with_geo\
        .withColumn('start_flag', F.when( (F.col('city') != F.lag('city').over(w1) ) | F.isnull(F.lag('city').over(w1)), 1))\
        .withColumn('end_flag', F.when( (F.col('city') != F.lead('city').over(w1) ) | F.isnull(F.lead('city').over(w1)), 1))\
        .withColumn('stay_id', F.count('start_flag').over(w1))
    
    #Step 2 - each stay with start and end
    w2 = Window().partitionBy('message_from', 'stay_id')
    
    travel_df = travel_df\
        .withColumn('city_stay_start', F.min('date').over(w2))\
        .withColumn('city_stay_end', F.max('date').over(w2))\
        .withColumn('city_stay_start_ts', F.min('message_ts').over(w2))\
        .withColumn("city_stay_len", F.datediff('city_stay_end', 'city_stay_start')+1)\
        .selectExpr('message_from as user_id', 'id as city_id', 'city', 'city_stay_start', 'city_stay_end', 'city_stay_len', 'city_stay_start_ts')\
        .distinct()\
        .withColumn('long_stay_flag', F.when(F.col('city_stay_len') >= 27, 1).otherwise(0))
    
    #Home city for stays over 27 days and travel chain
    w3 = Window.partitionBy('user_id').orderBy(F.desc('long_stay_flag'), F.desc('city_stay_start_ts'))

    travel_df = travel_df\
        .withColumn("home_city", F.when(F.first('long_stay_flag').over(w3) == 1, F.first('city').over(w3)))\
        .withColumn("home_city_id", F.when(F.first('long_stay_flag').over(w3) == 1, F.first('city_id').over(w3)))\
        .groupby("user_id").agg(
           F.sort_array(F.collect_list(F.struct("city_stay_start_ts", "city"))).alias("tuples_array"), \
           F.count('user_id').alias('travel_count'), \
           F.min('home_city').alias('home_city'), \
           F.min('home_city_id').alias('home_city_id'), \
           )\
           .withColumn("travel_array", F.col("tuples_array.city"))\
           .select('user_id', 'travel_count', 'travel_array', 'home_city', 'home_city_id')\
           .persist(StorageLevel.MEMORY_ONLY)
            
    return travel_df

def get_dmart_users(users_actual: DataFrame, users_travel: DataFrame):
    """
    Combines user location and travel data into a single DataFrame for downstream analysis.

    This function performs a full outer join on the `users_actual` and `users_travel` DataFrames 
    using the `user_id` column. The resulting DataFrame includes information about the user's 
    latest city and travel statistics.

    Args:
        users_actual (DataFrame): A DataFrame containing the latest location data for each user.
                                  Expected columns:
                                  - `user_id`: User identifier.
                                  - `act_city`: Latest city name.
                                  - `act_city_id`: Latest city identifier.
                                  - `local_time`: Local time of the user's last activity.

        users_travel (DataFrame): A DataFrame containing travel statistics for each user.
                                  Expected columns:
                                  - `user_id`: User identifier.
                                  - `travel_count`: Total number of city stays.
                                  - `travel_array`: Chronological list of visited cities.
                                  - `home_city`: User's home city.
                                  - `home_city_id`: Identifier of the home city.

    Returns:
        DataFrame: A DataFrame containing:
            - All columns from `users_actual`.
            - All columns from `users_travel`.
            - Rows for users present in either or both input DataFrames.

    Notes:
        - A full outer join ensures that users present in either `users_actual` or `users_travel` are included.
        - The function is lightweight and assumes the inputs are preprocessed and validated.

    """
    df = users_actual.join(users_travel, 'user_id', 'fullouter')
    return df

def writer(df: DataFrame, output_path: str):
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
        .parquet(f'{output_path}')

if __name__ == "__main__":
        main()