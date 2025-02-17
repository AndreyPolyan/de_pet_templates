import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def upsert_order(self
                     ,h_order_pk: str, order_id: int, order_dt: datetime # For HUB
                     ,load_dt: datetime, load_src: str # For HUB
                     ,order_cost: float, order_payment: float, order_status: str, #For Sattelite 
                     ) -> None:
        """
        Updating 
        HUB: dds.h_order
        SAT: dds.s_order_cost, dds.s_order_status
        """

        #HUB dds.h_order
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.h_order
                                    (h_order_pk, order_id, order_dt, load_dt, load_src)
                                VALUES
                                    (
                                        %(upsert_h_order_pk)s,
                                        %(upsert_order_id)s,
                                        %(upsert_order_dt)s,
                                        %(upsert_load_dt)s,
                                        %(upsert_load_src)s
                                    )
                                ON CONFLICT (h_order_pk) DO NOTHING;
                            """
                            , {
                                'upsert_h_order_pk': h_order_pk,
                                'upsert_order_id': order_id,
                                'upsert_order_dt': order_dt,
                                'upsert_load_dt': load_dt,
                                'upsert_load_src':load_src})

        # SAT dds.s_order_cost
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.s_order_cost
                                    (hk_order_cost_hashdiff, h_order_pk, cost, payment, load_dt, load_src)
                                VALUES
                                    (
                                        %(upsert_hk_order_cost_hashdiff)s,
                                        %(upsert_h_order_pk)s,
                                        %(upsert_cost)s,
                                        %(upsert_payment)s,
                                        %(upsert_load_dt)s,
                                        %(upsert_load_src)s
                                    )
                                ON CONFLICT (hk_order_cost_hashdiff) DO UPDATE
                                SET
                                    h_order_pk = EXCLUDED.h_order_pk,
                                    cost = EXCLUDED.cost,
                                    payment = EXCLUDED.payment,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """,{
                                'upsert_hk_order_cost_hashdiff':h_order_pk,
                                'upsert_h_order_pk':h_order_pk,
                                'upsert_cost':order_cost,
                                'upsert_payment':order_payment,
                                'upsert_load_dt':load_dt,
                                'upsert_load_src':load_src})

        #SAT dds.s_order_status
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.s_order_status
                                    (hk_order_status_hashdiff, h_order_pk, status, load_dt, load_src)
                                VALUES
                                    (
                                        %(upsert_hk_order_status_hashdiff)s,
                                        %(upsert_h_order_pk)s,
                                        %(upsert_status)s,
                                        %(upsert_load_dt)s,
                                        %(upsert_load_src)s
                                    )
                                ON CONFLICT (hk_order_status_hashdiff) DO UPDATE
                                SET
                                    h_order_pk = EXCLUDED.h_order_pk,
                                    status = EXCLUDED.status,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """, {
                                'upsert_hk_order_status_hashdiff':h_order_pk,
                                'upsert_h_order_pk':h_order_pk,
                                'upsert_status':order_status,
                                'upsert_load_dt':load_dt,
                                'upsert_load_src':load_src})
                
    def upsert_user(self
                    ,h_user_pk: str, user_id: str #For HUB
                    ,load_dt: datetime, load_src: str #For HUB
                    ,username: str, userlogin: str) -> None: # For Sattelite
        """
        Updating 
        HUB: dds.h_user
        SAT: dds.s_user_names
        """

        # HUB dds.h_user
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.h_user
                                    (h_user_pk, user_id, load_dt, load_src)
                                VALUES
                                    (
                                        %(upsert_h_user_pk)s,
                                        %(upsert_user_id)s,
                                        %(upsert_load_dt)s,
                                        %(upsert_load_src)s
                                    )
                                ON CONFLICT (h_user_pk) DO NOTHING;
                            """, {
                                'upsert_h_user_pk':h_user_pk,
                                'upsert_user_id':user_id,
                                'upsert_load_dt':load_dt,
                                'upsert_load_src':load_src

                            })

        # SAT dds.s_user_names
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.s_user_names
                                    (hk_user_names_hashdiff, h_user_pk, username, userlogin, load_dt, load_src)
                                VALUES
                                    (
                                        %(upsert_hk_user_names_hashdiff)s,
                                        %(upsert_h_user_pk)s,
                                        %(upsert_username)s,
                                        %(upsert_userlogin)s,
                                        %(upsert_load_dt)s,
                                        %(upsert_load_src)s
                                    )
                                ON CONFLICT (hk_user_names_hashdiff) DO UPDATE
                                SET
                                    h_user_pk = EXCLUDED.h_user_pk,
                                    username = EXCLUDED.username,
                                    userlogin = EXCLUDED.userlogin,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """, {
                                'upsert_hk_user_names_hashdiff':h_user_pk,
                                'upsert_h_user_pk':h_user_pk,
                                'upsert_username':username,
                                'upsert_userlogin':userlogin,
                                'upsert_load_dt':load_dt,
                                'upsert_load_src':load_src
                            })
    
    def upsert_restaurant(self
                          ,h_restaurant_pk: str, restaurant_id: str # For HUB
                          ,load_dt: datetime, load_src: str # For HUB
                          ,restaurant_name: str # For Sattelite
                          ) -> None:
        """
        Updating 
        HUB: dds.h_restaurant
        SAT: dds.s_restaurant_names
        """
    # HUB dds.h_restaurant
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.h_restaurant
                                    (h_restaurant_pk, restaurant_id, load_dt, load_src)
                                VALUES
                                    (
                                        %(h_restaurant_pk)s,
                                        %(restaurant_id)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (h_restaurant_pk) DO NOTHING;
                            """, {
                                'h_restaurant_pk': h_restaurant_pk,
                                'restaurant_id': restaurant_id,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })

    # SAT dds.s_restaurant_names
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.s_restaurant_names
                                    (hk_restaurant_names_hashdiff, h_restaurant_pk, name, load_dt, load_src)
                                VALUES
                                    (
                                        %(hk_restaurant_names_hashdiff)s,
                                        %(h_restaurant_pk)s,
                                        %(name)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (hk_restaurant_names_hashdiff) DO UPDATE
                                SET
                                    h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                                    name = EXCLUDED.name,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """, {
                                'hk_restaurant_names_hashdiff': h_restaurant_pk,
                                'h_restaurant_pk': h_restaurant_pk,
                                'name': restaurant_name,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })

    def upsert_category(self
                        ,h_category_pk: str # For HUB
                        ,load_dt: datetime, load_src: str # For HUB
                        ,category_name: str # For HUB
                        ) -> None:
        """
        Updating
        HUB: dds.h_category
        """
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.h_category
                                    (h_category_pk, category_name, load_dt, load_src)
                                VALUES
                                    (
                                        %(h_category_pk)s,
                                        %(category_name)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (h_category_pk) DO NOTHING;
                            """, {
                                'h_category_pk': h_category_pk,
                                'category_name': category_name,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })

    def upsert_product(self
                       ,h_product_pk: str, product_id: str # For HUB
                       ,load_dt: datetime, load_src: str # For HUB
                       ,product_name: str # For Sattelite
                       ) -> None:
        """
        Updating
        HUB: dds.h_product
        SAT: dds.s_product_names
        """
        # HUB dds.h_product
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.h_product
                                    (h_product_pk, product_id, load_dt, load_src)
                                VALUES
                                    (
                                        %(h_product_pk)s,
                                        %(product_id)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (h_product_pk) DO NOTHING;
                            """, {
                                'h_product_pk': h_product_pk,
                                'product_id': product_id,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })

        # SAT dds.s_product_names
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.s_product_names
                                    (hk_product_names_hashdiff, h_product_pk, name, load_dt, load_src)
                                VALUES
                                    (
                                        %(hk_product_names_hashdiff)s,
                                        %(h_product_pk)s,
                                        %(name)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (hk_product_names_hashdiff) DO UPDATE
                                SET
                                    h_product_pk = EXCLUDED.h_product_pk,
                                    name = EXCLUDED.name,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """, {
                                'hk_product_names_hashdiff': h_product_pk,
                                'h_product_pk': h_product_pk,
                                'name': product_name,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })

    def upsert_l_product_category(self
                                  ,hk_product_category_pk: str,h_product_pk: str, h_category_pk: str
                                  ,load_dt: datetime, load_src: str) -> None:
        """
        Updating
        LINK: dds.l_product_category
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.l_product_category
                                    (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                                VALUES
                                    (
                                        %(hk_product_category_pk)s,
                                        %(h_product_pk)s,
                                        %(h_category_pk)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (hk_product_category_pk) DO UPDATE
                                SET
                                    h_product_pk = EXCLUDED.h_product_pk,
                                    h_category_pk = EXCLUDED.h_category_pk,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """, {
                                'hk_product_category_pk': hk_product_category_pk,
                                'h_product_pk': h_product_pk,
                                'h_category_pk': h_category_pk,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })

    def upsert_l_product_restaurant(self
                                    ,hk_product_restaurant_pk: str,h_product_pk: str, h_restaurant_pk: str
                                    ,load_dt: datetime, load_src: str) -> None:
        """
        Updating
        LINK: dds.l_product_restaurant
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.l_product_restaurant
                                    (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                                VALUES
                                    (
                                        %(hk_product_restaurant_pk)s,
                                        %(h_product_pk)s,
                                        %(h_restaurant_pk)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                                SET
                                    h_product_pk = EXCLUDED.h_product_pk,
                                    h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """, {
                                'hk_product_restaurant_pk': hk_product_restaurant_pk,
                                'h_product_pk': h_product_pk,
                                'h_restaurant_pk': h_restaurant_pk,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })

    def upsert_l_order_product(self
                               , hk_order_product_pk: str,h_order_pk: str, h_product_pk: str
                               ,load_dt: datetime, load_src: str) -> None:
        """
        Updating
        LINK: dds.l_order_product
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.l_order_product
                                    (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                                VALUES
                                    (
                                        %(hk_order_product_pk)s,
                                        %(h_order_pk)s,
                                        %(h_product_pk)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (hk_order_product_pk) DO UPDATE
                                SET
                                    h_order_pk = EXCLUDED.h_order_pk,
                                    h_product_pk = EXCLUDED.h_product_pk,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """, {
                                'hk_order_product_pk': hk_order_product_pk,
                                'h_order_pk': h_order_pk,
                                'h_product_pk': h_product_pk,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })

    def upsert_l_order_user(self
                            ,hk_order_user_pk: str,h_order_pk: str, h_user_pk: str
                            ,load_dt: datetime, load_src: str) -> None:
        """
        Updating
        LINK: dds.l_order_user
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                                INSERT INTO dds.l_order_user
                                    (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                                VALUES
                                    (
                                        %(hk_order_user_pk)s,
                                        %(h_order_pk)s,
                                        %(h_user_pk)s,
                                        %(load_dt)s,
                                        %(load_src)s
                                    )
                                ON CONFLICT (hk_order_user_pk) DO UPDATE
                                SET
                                    h_order_pk = EXCLUDED.h_order_pk,
                                    h_user_pk = EXCLUDED.h_user_pk,
                                    load_dt = EXCLUDED.load_dt,
                                    load_src = EXCLUDED.load_src;
                            """, {
                                'hk_order_user_pk': hk_order_user_pk,
                                'h_order_pk': h_order_pk,
                                'h_user_pk': h_user_pk,
                                'load_dt': load_dt,
                                'load_src': load_src
                            })