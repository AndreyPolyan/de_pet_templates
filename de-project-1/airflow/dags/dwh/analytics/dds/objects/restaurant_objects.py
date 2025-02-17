from pydantic import BaseModel
from datetime import datetime


class RestaurantJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str

class RestaurantDdsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime
    def __eq__(self, other):
        return \
            self.restaurant_id == other.restaurant_id\
            and self.restaurant_name == other.restaurant_name\
            and type(self) == type(other)