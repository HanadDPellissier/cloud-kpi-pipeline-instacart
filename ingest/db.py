import os
from dotenv import load_dotenv
import psycopg

from config.settings import PG_HOST, PG_PORT, PG_DB, PG_USER

load_dotenv()

def get_conn():
    pw = os.getenv("PG_PASSWORD")
    if not pw:
        raise RuntimeError("PG_PASSWORD missing. Add it to your .env file.")
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=pw,
        autocommit=True,
    )