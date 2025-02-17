from datetime import datetime, date, time
from pydantic import BaseModel


class TsDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int 
    day: int 
    time: time
    date: date