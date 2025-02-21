from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable

from .vertica_connect import VerticaConnect
from .s3_connect import S3Connect


class ConnectionBuilder:
    @staticmethod
    def vertica_conn(conn_id: str) -> VerticaConnect:
        conn = BaseHook.get_connection(conn_id)


        vc = VerticaConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password))

        return vc
    
    @staticmethod
    def s3_conn(aws_access_key_var: str, aws_secret_key_var: str, aws_endpoint_var: str) -> S3Connect:
        try:
            aws_access_key = Variable.get(aws_access_key_var)
        except Exception as e:
            print(f'S3 Access Key is not set.')
            raise e
        try:
            aws_secret_key = Variable.get(aws_secret_key_var)
        except Exception as e:
            print(f'S3 Secret Key is not set.')
            raise e
        try:
            aws_endpoint = Variable.get(aws_endpoint_var)
        except Exception as e:
            print(f'S3 End Point is not set.')
            raise e
        
        s3 = S3Connect(aws_access_key, aws_secret_key, aws_endpoint)

        return s3