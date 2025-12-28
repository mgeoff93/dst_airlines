import os
import logging
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)

class PostgresClient:
    _pool = None
    
    @classmethod
    def get_pool(cls):
        if cls._pool is None:
            cls._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                database=os.getenv("POSTGRES_DB", "airlines"),
                user=os.getenv("POSTGRES_USER", "airflow"),
                password=os.getenv("POSTGRES_PASSWORD", "airflow")
            )
            logging.info("PostgreSQL connection pool created")
        return cls._pool
    
    @contextmanager
    def get_connection(self):
        pool = self.get_pool()
        conn = pool.getconn()
        try:
            yield conn
        finally:
            pool.putconn(conn)
    
    def query(self, sql, params=None):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params or ())
                return cur.fetchall()
