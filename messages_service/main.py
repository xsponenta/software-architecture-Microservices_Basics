import asyncio
import json
import os
import re
import threading
import time

import hazelcast
import httpx
import psycopg2
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Counter Service")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "transactions")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8003")
SERVICE_NAME = os.getenv("SERVICE_NAME", "counter-service")
SERVICE_ADDRESS = os.getenv("SERVICE_ADDRESS", "counter-service:8002")
COUNTER_QUEUE_NAME = os.getenv("COUNTER_QUEUE_NAME", "counter-transactions")
HAZELCAST_CLUSTER_NAME = os.getenv("HAZELCAST_CLUSTER_NAME", "dev")
HAZELCAST_MEMBERS = [
    member.strip()
    for member in os.getenv(
        "HAZELCAST_MEMBERS",
        "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
    ).split(",")
    if member.strip()
]

hazelcast_client = None
counter_queue = None
stop_consumer = threading.Event()


class MessageIn(BaseModel):
    uuid: str
    msg: str
    account: str | None = None
    amount: float | None = None


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def wait_for_db():
    for attempt in range(30):
        try:
            conn = get_conn()
            conn.close()
            return
        except psycopg2.OperationalError as exc:
            print(f"[counter-service] waiting for PostgreSQL ({attempt + 1}/30): {exc}")
            time.sleep(1)
    raise RuntimeError("PostgreSQL is unavailable")


def init_db():
    wait_for_db()
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    uuid TEXT PRIMARY KEY,
                    msg TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT 'default'")
            cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS amount NUMERIC NOT NULL DEFAULT 0")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS balances (
                    account_id TEXT PRIMARY KEY,
                    balance NUMERIC NOT NULL DEFAULT 0
                )
                """
            )
        conn.commit()
        print("[counter-service] PostgreSQL initialized")
    finally:
        conn.close()


def parse_transaction(data: MessageIn) -> tuple[str, float]:
    account = data.account
    amount = data.amount

    if not account:
        account_match = re.search(r"(?:account|acc)[-_:\s]*([A-Za-z0-9_-]+)", data.msg, re.IGNORECASE)
        account = f"account-{account_match.group(1)}" if account_match else "default"

    if amount is None:
        numbers = re.findall(r"-?\d+(?:\.\d+)?", data.msg)
        amount = float(numbers[-1]) if numbers else 1.0

    return account, float(amount)


def apply_transaction(data: MessageIn) -> str:
    account, amount = parse_transaction(data)
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO transactions (uuid, msg, account_id, amount)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (uuid) DO NOTHING
                """,
                (data.uuid, data.msg, account, amount),
            )
            inserted = cursor.rowcount > 0
            if inserted:
                cursor.execute(
                    """
                    INSERT INTO balances (account_id, balance)
                    VALUES (%s, %s)
                    ON CONFLICT (account_id)
                    DO UPDATE SET balance = balances.balance + EXCLUDED.balance
                    """,
                    (account, amount),
                )
        conn.commit()
        print(
            f"[counter-service] {'applied' if inserted else 'duplicate'} "
            f"uuid={data.uuid} account={account} amount={amount}"
        )
        return "stored" if inserted else "duplicate"
    finally:
        conn.close()


def queue_consumer():
    print(f"[counter-service] consuming Hazelcast queue '{COUNTER_QUEUE_NAME}'")
    while not stop_consumer.is_set():
        try:
            raw = counter_queue.take()
            payload = json.loads(raw) if isinstance(raw, str) else raw
            apply_transaction(MessageIn(**payload))
        except Exception as exc:
            print(f"[counter-service] queue consumer error: {exc}")
            time.sleep(1)


async def register_self():
    payload = {"name": SERVICE_NAME, "address": SERVICE_ADDRESS}
    async with httpx.AsyncClient() as client:
        for attempt in range(30):
            try:
                response = await client.post(f"{CONFIG_SERVER_URL}/register", json=payload, timeout=2)
                response.raise_for_status()
                print(f"[counter-service] registered in config-server as {SERVICE_ADDRESS}")
                return
            except Exception as exc:
                print(f"[counter-service] config-server registration retry {attempt + 1}: {exc}")
                await asyncio.sleep(1)


def init_queue():
    global hazelcast_client, counter_queue
    hazelcast_client = hazelcast.HazelcastClient(
        cluster_name=HAZELCAST_CLUSTER_NAME,
        cluster_members=HAZELCAST_MEMBERS,
    )
    counter_queue = hazelcast_client.get_queue(COUNTER_QUEUE_NAME).blocking()
    threading.Thread(target=queue_consumer, daemon=True).start()


@app.on_event("startup")
async def startup():
    init_db()
    await register_self()
    await asyncio.to_thread(init_queue)


@app.on_event("shutdown")
def shutdown():
    stop_consumer.set()
    if hazelcast_client:
        hazelcast_client.shutdown()


@app.post("/messages")
async def add_message(data: MessageIn):
    status = await asyncio.to_thread(apply_transaction, data)
    return {"status": status, "uuid": data.uuid}


@app.get("/messages")
async def get_messages():
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT uuid, msg, account_id, amount, created_at FROM transactions ORDER BY created_at ASC"
            )
            transaction_rows = cursor.fetchall()
            cursor.execute("SELECT account_id, balance FROM balances ORDER BY account_id ASC")
            balance_rows = cursor.fetchall()
        return {
            "count": len(transaction_rows),
            "transactions": [
                {
                    "uuid": row[0],
                    "msg": row[1],
                    "account": row[2],
                    "amount": float(row[3]),
                    "created_at": row[4].isoformat(),
                }
                for row in transaction_rows
            ],
            "balances": {row[0]: float(row[1]) for row in balance_rows},
        }
    finally:
        conn.close()
