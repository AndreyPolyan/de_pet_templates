from datetime import  datetime
from pydantic import BaseModel


class ProductDdsObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int
    def __eq__(self, other):
        return \
            self.product_id == other.product_id\
            and self.product_name == other.product_name\
            and self.product_price == other.product_price
    
class BonusProductDdsObj(BaseModel):
    product_id: str
    bonus_payment: float
    bonus_grant: float