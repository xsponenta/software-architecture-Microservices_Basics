import uuid
import grpc
import httpx
import asyncio
import os
import random
import time
import json
import hazelcast
from fastapi import FastAPI
from pydantic import BaseModel

import logging_pb2
import logging_pb2_grpc

app = FastAPI()

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8003")
SERVICE_NAME = os.getenv("SERVICE_NAME", "facade-service")
SERVICE_ADDRESS = os.getenv("SERVICE_ADDRESS", "facade-service:8000")
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

class Msg(BaseModel):
    msg: str
    account: str | None = None
    amount: float | None = None


async def register_self():
    payload = {"name": SERVICE_NAME, "address": SERVICE_ADDRESS}
    async with httpx.AsyncClient() as client:
        for attempt in range(30):
            try:
                response = await client.post(f"{CONFIG_SERVER_URL}/register", json=payload, timeout=2)
                response.raise_for_status()
                print(f"[facade-service] registered in config-server as {SERVICE_ADDRESS}")
                return
            except Exception as exc:
                print(f"[facade-service] config-server registration retry {attempt + 1}: {exc}")
                await asyncio.sleep(1)


async def discover(service_name: str) -> list[str]:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{CONFIG_SERVER_URL}/services/{service_name}", timeout=3)
        response.raise_for_status()
        addresses = response.json()["addresses"]
        random.shuffle(addresses)
        return addresses


def _init_queue():
    global hazelcast_client, counter_queue
    hazelcast_client = hazelcast.HazelcastClient(
        cluster_name=HAZELCAST_CLUSTER_NAME,
        cluster_members=HAZELCAST_MEMBERS,
    )
    counter_queue = hazelcast_client.get_queue(COUNTER_QUEUE_NAME).blocking()
    print(f"[facade-service] connected to Hazelcast queue '{COUNTER_QUEUE_NAME}'")


@app.on_event("startup")
async def startup():
    await register_self()
    await asyncio.to_thread(_init_queue)


@app.on_event("shutdown")
def shutdown():
    if hazelcast_client:
        hazelcast_client.shutdown()


def _get_randomized_grpc_addresses(addresses: list[str]) -> list[str]:
    addresses = addresses.copy()
    random.shuffle(addresses)
    return addresses


def grpc_send_log_with_failover(addresses: list[str], msg_uuid: str, text: str):
    last_error = None
    for addr in _get_randomized_grpc_addresses(addresses):
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = logging_pb2_grpc.LoggingServiceStub(channel)
                response = stub.Log(
                    logging_pb2.LogRequest(uuid=msg_uuid, msg=text),
                    timeout=20,
                )
                return response.status, addr
        except grpc.RpcError as exc:
            print(f"[WARN] Logging node unavailable: {addr} ({exc.code()})")
            last_error = exc

    if last_error:
        raise last_error
    raise RuntimeError("No logging addresses configured")


def grpc_get_logs_with_failover(addresses: list[str]):
    last_error = None
    for addr in _get_randomized_grpc_addresses(addresses):
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = logging_pb2_grpc.LoggingServiceStub(channel)
                response = stub.GetLogs(logging_pb2.Empty(), timeout=20)
                return response.logs, addr
        except grpc.RpcError as exc:
            print(f"[WARN] Logging node unavailable: {addr} ({exc.code()})")
            last_error = exc

    if last_error:
        raise last_error
    raise RuntimeError("No logging addresses configured")


def enqueue_counter_message(payload: dict):
    if not counter_queue:
        raise RuntimeError("Counter queue is not initialized")
    counter_queue.put(json.dumps(payload))


@app.get("/")
async def root():
    return {
        "service": "Facade Service",
        "endpoints": {
            "RECEIVE /receive": "Get combined logs and messages",
            "SEND /send": "Send a message (requires JSON body: {'msg': 'your message'})"
        },
        "architecture": {
            "port_8000": "Facade Service",
            "port_8002": "Counter Service",
            "port_8003": "Config Server",
            "port_50051": "Logging Service (gRPC)"
        }
    }

@app.post("/send")
async def send_msg(data: Msg):
    msg_id = str(uuid.uuid4())
    start = time.perf_counter()
    
    try:
        logging_addrs = await discover("logging-service")
        status, selected_addr = await asyncio.to_thread(
            grpc_send_log_with_failover,
            logging_addrs,
            msg_id,
            data.msg,
        )

        await asyncio.to_thread(
            enqueue_counter_message,
            {
                "uuid": msg_id,
                "msg": data.msg,
                "account": data.account,
                "amount": data.amount,
            },
        )

        return {
            "uuid": msg_id,
            "status": status,
            "logging_instance": selected_addr,
            "counter_status": "queued",
            "elapsed_ms": round((time.perf_counter() - start) * 1000, 2),
        }
    except Exception as e:
        print(f"[ERROR] Retry failed: {e}")
        return {"uuid": msg_id, "status": "failed", "error": str(e)}

@app.get("/receive")
async def get_combined():
    try:
        logging_addrs = await discover("logging-service")
        logs_str, selected_addr = await asyncio.to_thread(grpc_get_logs_with_failover, logging_addrs)
    except grpc.RpcError:
        logs_str = "<logging unavailable>"
        selected_addr = "<none>"
    except Exception as exc:
        print(f"[WARN] logging discovery failed: {exc}")
        logs_str = "<logging unavailable>"
        selected_addr = "<none>"

    counter_payload = {"transactions": None, "balances": None}
    try:
        counter_addrs = await discover("counter-service")
        selected_counter = random.choice(counter_addrs)
        async with httpx.AsyncClient() as client:
            msg_resp = await client.get(f"http://{selected_counter}/messages", timeout=3)
            msg_resp.raise_for_status()
            counter_payload = msg_resp.json()
            counter_payload["counter_instance"] = selected_counter
    except Exception as exc:
        print(f"[WARN] counter-service unavailable for GET: {exc}")

    if logs_str and logs_str != "<logging unavailable>":
        logs_list = [log.strip() for log in logs_str.split(",") if log.strip()]
        logs_formatted = "\n".join(logs_list)
    else:
        logs_formatted = ""
    
    return {
        "logging_instance": selected_addr,
        "logs": logs_formatted,
        "counter": counter_payload
    }
