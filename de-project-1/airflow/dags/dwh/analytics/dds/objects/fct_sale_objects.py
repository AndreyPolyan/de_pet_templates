from datetime import datetime
from pydantic import BaseModel

class FactSaleDdsObj(BaseModel):
    id: int
    product_id: int
    product_key: str 
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class FactBonusSaleJsonObj(BaseModel):
    id: int
    event_ts: datetime
    event_value: str

class BonusProductDdsObj(BaseModel):
    product_id: str
    bonus_payment: float
    bonus_grant: float