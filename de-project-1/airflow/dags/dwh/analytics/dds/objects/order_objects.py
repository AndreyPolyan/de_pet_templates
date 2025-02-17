from pydantic import BaseModel


class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str

class OrderDdsObj(BaseModel):
    id: int 
    order_id: str
    order_status: str
    restaurant_id: int 
    timestamp_id: int
    user_id: int
    def __eq__(self, other):
        return \
        self.order_id == other.order_id \
        and self.order_status == other.order_status \
        and self.restaurant_id == other.restaurant_id \
        and self.timestamp_id == other.timestamp_id \
        and self.user_id == other.user_id