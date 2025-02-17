from psycopg import Connection
from psycopg.rows import class_row
from datetime import datetime
from typing import List, Optional

from analytics.dds.objects import TsDdsObj


class TsDdsRepository:
    def insert_dds_ts(self, conn: Connection, ts: TsDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                    """
                        INSERT INTO dds.dm_timestamps(
                            ts,
                            year,
                            month,
                            day,
                            time,
                            date)
                        VALUES (
                            %(ts)s,
                            %(year)s,
                            %(month)s,
                            %(day)s,
                            %(time)s,
                            %(date)s);
                    """,
                    {
                        "ts": ts.ts,
                        "year": ts.year,
                        "month": ts.month,
                        "day": ts.day,
                        "time": ts.time,
                        "date": ts.date
                    },
                )
                
    def get_ts(self, conn: Connection, ts: datetime) -> Optional[TsDdsObj]:
        with conn.cursor(row_factory=class_row(TsDdsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id
                        ,ts
                        ,year
                        ,month
                        ,day
                        ,time
                        ,date
                    FROM dds.dm_timestamps
                    WHERE ts = %(ts)s;
                """,
                {"ts": ts},
            )
            obj = cur.fetchone()
        return obj
    def list_ts(self, conn: Connection) -> List[TsDdsObj]:
        with conn.cursor(row_factory=class_row(TsDdsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id
                        ,ts
                        ,year
                        ,month
                        ,day
                        ,time
                        ,date
                    FROM dds.dm_timestamps;
                """
            )
            obj = cur.fetchall()
        return obj