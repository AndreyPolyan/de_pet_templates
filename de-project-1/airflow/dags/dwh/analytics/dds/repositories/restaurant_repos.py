from psycopg import Connection
from psycopg.rows import class_row
from datetime import datetime
from typing import List, Optional

from analytics.dds.objects import RestaurantJsonObj, RestaurantDdsObj


class RestaurantRawRepository:
    def load_raw_restaurants(self, conn: Connection, last_loaded_ts: datetime) -> List[RestaurantJsonObj]:
        with conn.cursor(row_factory=class_row(RestaurantJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > %(last_loaded_ts)s;
                """,
                {"last_loaded_ts": last_loaded_ts},
            )
            objs = cur.fetchall()
        return objs
    

class RestaurantDdsRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s);
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                },
            )

    def update_restaurant(self, conn: Connection, restaurant: RestaurantDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE dds.dm_restaurants
                    SET 
                        restaurant_name = %(restaurant_name)s
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name
                },
            )

    def get_restaurant(self, conn: Connection, restaurant_id: str) -> Optional[RestaurantDdsObj]:
        with conn.cursor(row_factory=class_row(RestaurantDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        restaurant_id,
                        restaurant_name,
                        active_from,
                        active_to
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj