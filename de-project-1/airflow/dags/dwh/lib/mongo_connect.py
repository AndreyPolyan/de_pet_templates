from urllib.parse import quote_plus as quote

from pymongo.mongo_client import MongoClient


class MongoConnect:
    def __init__(self, **kwargs) -> None:
        """
        Params:
            user
            pw
            host
            replica_set
            auth_db
            main_db
            cert_path
        """
        self.user = kwargs.get('user')
        self.pw = kwargs.get('pw')
        self.host = kwargs.get('host')
        self.replica_set = kwargs.get('rs')
        self.auth_db = kwargs.get('auth_db')
        self.main_db = kwargs.get('main_db')
        self.cert_path = kwargs.get('cert_path')

    def url(self, simplified = True) -> str:
        if simplified:
            return 'mongodb://{user}:{pw}@{hosts}/'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=self.host)
        else: 
            return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
                user=quote(self.user),
                pw=quote(self.pw),
                hosts=self.host,
                rs=self.replica_set,
                auth_src=self.auth_db)

    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]
