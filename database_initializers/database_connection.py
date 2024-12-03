import psycopg2
from contextlib import contextmanager
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )

@contextmanager
def postgresql_db_cursor(host, database, username, password, port_id=5432):
    connection = psycopg2.connect(
        host = host,
        database = database,
        user = username,
        password = password,
        port = port_id
    )
    try:
        with connection.cursor() as cur:
            yield cur
        connection.commit()
    except Exception as e:
        logging.error(f"An Error occurred while getting the connection {e}")
        connection.rollback()
    finally:
        connection.close()

