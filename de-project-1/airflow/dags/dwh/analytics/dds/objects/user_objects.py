from pydantic import BaseModel


class UserDdsObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str
    def __eq__(self, other):
        return \
            self.user_id == other.user_id\
            and self.user_name == other.user_name\
            and type(self) == type(other)

class UserJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str