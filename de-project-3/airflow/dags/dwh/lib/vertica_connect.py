from contextlib import contextmanager
from typing import Dict, Generator

import vertica_python


class VerticaConnect:
    """Handles connection to a Vertica database."""

    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str) -> None:
        """
        Initialize the Vertica connection with required credentials.

        Args:
            host (str): Hostname or IP address of the Vertica database.
            port (str): Port number for the Vertica connection.
            db_name (str): Name of the Vertica database.
            user (str): Username for authentication.
            pw (str): Password for authentication.
        """
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw

    def conn_info(self, autocommit: bool = False) -> Dict:
        """
        Get connection parameters for Vertica.

        Args:
            autocommit (bool, optional): Whether to enable autocommit. Defaults to False.

        Returns:
            Dict: Connection parameters for Vertica.
        """
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.pw,
            "database": self.db_name,
            "autocommit": autocommit,
        }

    def client(self, autocommit: bool = False) -> vertica_python.Connection:
        """
        Create and return a new Vertica connection.

        Args:
            autocommit (bool, optional): Whether to enable autocommit. Defaults to False.

        Returns:
            vertica_python.Connection: A connection object to the Vertica database.
        """
        return vertica_python.connect(**self.conn_info(autocommit=autocommit))

    @contextmanager
    def connection(
        self, autocommit: bool = False
    ) -> Generator[vertica_python.Connection, None, None]:
        """
        Context manager for a Vertica database connection.

        Ensures automatic commit on success and rollback on failure.

        Args:
            autocommit (bool, optional): Whether to enable autocommit. Defaults to False.

        Yields:
            vertica_python.Connection: Active Vertica connection.

        Raises:
            Exception: If an error occurs, the transaction is rolled back.
        """
        conn = vertica_python.connect(**self.conn_info(autocommit=autocommit))
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()