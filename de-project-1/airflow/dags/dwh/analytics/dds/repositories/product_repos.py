from psycopg import Connection
from psycopg.rows import class_row
from datetime import datetime
from typing import List, Optional

from analytics.dds.objects import ProductDdsObj,RestaurantDdsObj


class ProductDdsRepository:
    def insert_dds_product(self, conn: Connection, product: ProductDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                    """
                        INSERT INTO dds.dm_products(
                            product_id,
                            product_name,
                            product_price,
                            active_from,
                            active_to,
                            restaurant_id)
                        VALUES (
                            %(product_id)s,
                            %(product_name)s,
                            %(product_price)s,
                            %(active_from)s,
                            %(active_to)s,
                            %(restaurant_id)s);
                    """,
                    {
                        "product_id": product.product_id,
                        "product_name": product.product_name,
                        "product_price": product.product_price,
                        "active_from": product.active_from,
                        "active_to": product.active_to,
                        "restaurant_id": product.restaurant_id
                    },
                )
                

    def get_product(self, conn: Connection, product_id: str) -> Optional[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id
                        ,product_id
                        ,product_name
                        ,product_price
                        ,active_from
                        ,active_to
                        ,restaurant_id
                    from 
                        (SELECT 
                            id, product_id, product_name, product_price
                            ,active_from, active_to, restaurant_id
                            ,row_number() over(partition by product_id order by COALESCE(active_to,'2099-12-31'::date) desc) rnk
                        FROM dds.dm_products
                        WHERE product_id = %(product_id)s
                        ) sbq
                    WHERE rnk = 1;
                """,
                {"product_id": product_id},
            )
            obj = cur.fetchone()
        return obj
    def get_product_version(self, conn: Connection, product_id: str, last_ts: datetime) -> Optional[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id
                        ,product_id
                        ,product_name
                        ,product_price
                        ,active_from
                        ,active_to
                        ,restaurant_id
                    from 
                        (SELECT 
                            id, product_id, product_name, product_price
                            ,active_from, active_to, restaurant_id
                            ,row_number() over(partition by product_id order by COALESCE(active_to,'2099-12-31'::date) desc) rnk
                        FROM dds.dm_products
                        WHERE true
                        and product_id = %(product_id)s
                        and active_from <= %(last_ts)s
                        ) sbq
                    WHERE rnk = 1;
                """,
                {
                    "product_id": product_id,
                    "last_ts": last_ts
                    },
            )
            obj = cur.fetchone()
        return obj
    def update_product(self, conn: Connection, product: ProductDdsObj, active_to: datetime) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE dds.dm_product
                    SET 
                        active_to = %(active_to)s,
                    WHERE id = %(id)s;
                """,
                {
                    "active_to": active_to,
                    "id": product.id
                },
            )

    def list_actual_products(self, conn: Connection, restaurant: RestaurantDdsObj) -> List[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id
                        ,product_id
                        ,product_name
                        ,product_price
                        ,active_from
                        ,active_to
                        ,restaurant_id
                    from 
                        (SELECT 
                            id, product_id, product_name, product_price
                            ,active_from, active_to, restaurant_id
                            ,row_number() over(partition by product_id order by COALESCE(active_to,'2099-12-31'::date) desc) rnk
                        FROM dds.dm_products
                        WHERE restaurant_id = %(restaurant_id)s
                        and COALESCE(active_to,'2099-12-31'::date)>= now()::date
                        ) sbq
                    WHERE rnk = 1;
                """,
                {
                    "restaurant_id": restaurant.id
                }
            )
            obj = cur.fetchall()
        return obj
    def list_all_products(self, conn: Connection) -> List[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id
                        ,product_id
                        ,product_name
                        ,product_price
                        ,active_from
                        ,active_to
                        ,restaurant_id
                    from 
                        (SELECT 
                            id, product_id, product_name, product_price
                            ,active_from, active_to, restaurant_id
                            ,row_number() over(partition by product_id order by COALESCE(active_to,'2099-12-31'::date) desc) rnk
                        FROM dds.dm_products
                        WHERE true
                        and COALESCE(active_to,'2099-12-31'::date)>= now()::date
                        ) sbq
                    WHERE rnk = 1;
                """
            )
            obj = cur.fetchall()
        return obj
