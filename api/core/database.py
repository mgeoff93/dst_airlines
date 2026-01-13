import logging
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from api.core.config import POSTGRES_HOST, POSTGRES_PORT, AIRLINES_POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

logging.basicConfig(level = logging.INFO)

class PostgresClient:
	_pool = None

	@classmethod
	def get_pool(cls):
		if cls._pool is None:
			cls._pool = pool.ThreadedConnectionPool(
				minconn=1,
				maxconn=10,
				host=POSTGRES_HOST,
				port=POSTGRES_PORT,
				database=AIRLINES_POSTGRES_DB,
				user=POSTGRES_USER,
				password=POSTGRES_PASSWORD
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

# --- instance globale à utiliser dans tous les routers ---
db = PostgresClient()