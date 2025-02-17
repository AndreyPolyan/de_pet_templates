from psycopg import Connection
from psycopg.rows import class_row
from datetime import datetime
from typing import List, Optional

from analytics.dds.objects import FactBonusSaleJsonObj, FactSaleDdsObj


class FactSaleDdsRepository:
    def insert_fctsales(self, conn: Connection, fctsales: List[FactSaleDdsObj]) -> None:
        with conn.cursor() as cur:
            for fctsale in fctsales:
                cur.execute(
                    """
                        INSERT INTO dds.fct_product_sales(
                            product_id,
                            order_id,
                            count,
                            price,
                            total_sum,
                            bonus_payment,
                            bonus_grant)
                        VALUES (
                            %(product_id)s,
                            %(order_id)s,
                            %(count)s,
                            %(price)s,
                            %(total_sum)s,
                            %(bonus_payment)s,
                            %(bonus_grant)s);
                    """,
                    {
                        "product_id": fctsale.product_id,
                        "order_id": fctsale.order_id,
                        "count": fctsale.count,
                        "price": fctsale.price,
                        "total_sum": fctsale.total_sum,
                        "bonus_payment": fctsale.bonus_payment,
                        "bonus_grant": fctsale.bonus_grant
                    },
                )
                

    def get_fctsale_by_order(self, conn: Connection, order_id: int) -> Optional[FactSaleDdsObj]:
        with conn.cursor(row_factory=class_row(FactSaleDdsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id
                        ,product_id
                        ,order_id
                        ,count
                        ,price
                        ,total_sum
                        ,bonus_payment
                        ,bonus_grant
                    FROM dds.fct_product_sales
                        WHERE order_id = %(order_id)s
                        ) sbq
                    WHERE rnk = 1;
                """,
                {"fctsale_id": order_id},
            )
            obj = cur.fetchone()
        return obj

class BonusRawRepository:
    def load_raw_events(self, conn: Connection, last_loaded_ts: datetime) -> List[FactBonusSaleJsonObj]:
        with conn.cursor(row_factory=class_row(FactBonusSaleJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        event_ts,
                        event_value
                    FROM stg.bonussystem_events
                    WHERE true
                    and event_type = 'bonus_transaction'
                    and event_ts > %(last_loaded_ts)s;
                """,
                {"last_loaded_ts": last_loaded_ts},
            )
            objs = cur.fetchall()
        return objs
    def get_event_by_order(self, conn: Connection, order_id: str) -> Optional[FactBonusSaleJsonObj]:
        with conn.cursor(row_factory=class_row(FactBonusSaleJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        event_ts,
                        event_value
                    FROM stg.bonussystem_events
                    WHERE true 
                    and event_type = 'bonus_transaction'
                    and replace((event_value::json->'order_id')::varchar, '"','') = %(order_id)s
                """,
                {"order_id": order_id},
            )
            obj = cur.fetchone()
        return obj