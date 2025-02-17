from psycopg import Connection
from psycopg.rows import class_row
from datetime import datetime
from typing import List, Optional


from analytics.dds.objects import OrderJsonObj, OrderDdsObj


class OrderRawRepository:
    def load_raw_orders(self, conn: Connection, last_loaded_ts: datetime, limit: int = 10000) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %(last_loaded_ts)s
                    ORDER BY update_ts ASC
                    LIMIT %(limit)s;
                """,
                {
                    "last_loaded_ts": last_loaded_ts,
                    "limit": limit
                 },
            )
            objs = cur.fetchall()
        return objs
    
    def load_raw_orders_by_id(self, conn: Connection, event_threshold: int, limit: int = 10000) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_orders
                    WHERE id > %(event_threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {
                    "event_threshold": event_threshold,
                    "limit": limit
                 },
            )
            objs = cur.fetchall()
        return objs

class OrderDdsRepository:
    def insert_order(self, conn: Connection, order: OrderDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_orders(
                    order_id,
                    order_status,
                    restaurant_id,
                    timestamp_id,
                    user_id)
                VALUES 
                    (
                    %(order_id)s,
                    %(order_status)s,
                    %(restaurant_id)s,
                    %(timestamp_id)s,
                    %(user_id)s);
                """,
                {
                    "order_id": order.order_id,
                    "order_status": order.order_status,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "user_id": order.user_id
                }
            )
    def get_order(self, conn: Connection, order_id: str) -> Optional[OrderDdsObj]:
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                SELECT
                    id,
                    order_id,
                    order_status,
                    restaurant_id,
                    timestamp_id,
                    user_id
                FROM 
                    dds.dm_orders 
                WHERE 
                    order_id = %(order_id)s
                """,
                {
                    "order_id":order_id
                }
            )
            obj = cur.fetchone()
        return obj
    def update_order(self, conn: Connection, order: OrderDdsObj) -> Optional[OrderDdsObj]:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dds.dm_orders 
                SET order_status = %(order_status)s
                WHERE 
                    order_id = %(order_id)s
                """,
                {
                    "order_id":order.order_id,
                    "order_status": order.order_status
                }
            )