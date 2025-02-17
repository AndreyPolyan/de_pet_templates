from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class MgCollectionReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_collection(self, load_threshold: datetime, limit, collection_name:str = None) -> List[Dict]:
        if not collection_name:
            raise NameError('Collection is not set')
        
        filter = {'update_ts': {'$gt': load_threshold}}

        sort = [('update_ts', 1)]

        docs = list(self.dbs.get_collection(collection_name).find(filter=filter, sort=sort, limit=limit))
        return docs
