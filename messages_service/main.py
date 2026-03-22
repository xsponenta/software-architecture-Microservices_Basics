from fastapi import FastAPI
import os
import psycopg2
from pydantic import BaseModel

app = FastAPI()

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "transactions")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


class MessageIn(BaseModel):
    uuid: str
    msg: str


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


@app.on_event("startup")
def init_db():
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    uuid TEXT PRIMARY KEY,
                    msg TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        conn.commit()
        print("[messages-service] PostgreSQL initialized")
    finally:
        conn.close()


@app.post("/messages")
async def add_message(data: MessageIn):
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO transactions (uuid, msg) VALUES (%s, %s) ON CONFLICT (uuid) DO NOTHING",
                (data.uuid, data.msg),
            )
            inserted = cursor.rowcount > 0
        conn.commit()
        return {"status": "stored" if inserted else "duplicate", "uuid": data.uuid}
    finally:
        conn.close()


@app.get("/messages")
async def get_messages():
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT uuid, msg, created_at FROM transactions ORDER BY created_at ASC")
            rows = cursor.fetchall()
        return {
            "count": len(rows),
            "messages": [
                {"uuid": row[0], "msg": row[1], "created_at": row[2].isoformat()} for row in rows
            ],
        }
    finally:
        conn.close()