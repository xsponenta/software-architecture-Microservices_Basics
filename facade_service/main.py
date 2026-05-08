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
from consul_utils import discover_service, get_csv_kv, get_kv, register_service

app = FastAPI()

SERVICE_NAME = os.getenv("SERVICE_NAME", "facade-service")
SERVICE_ID = os.getenv("SERVICE_ID", SERVICE_NAME)
SERVICE_ADDRESS = os.getenv("SERVICE_ADDRESS", "facade-service:8000")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))

hazelcast_client = None
counter_queue = None
counter_queue_name = None

class Msg(BaseModel):
    msg: str
    account: str | None = None
    amount: float | None = None


async def discover(service_name: str) -> list[str]:
    return await discover_service(service_name)


def _init_queue():
    global hazelcast_client, counter_queue, counter_queue_name
    cluster_name = get_kv("config/hazelcast/cluster_name", "dev")
    members = get_csv_kv(
        "config/mq/hazelcast_members",
        "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
    )
    counter_queue_name = get_kv("config/mq/queue_name", "counter-transactions")
    hazelcast_client = hazelcast.HazelcastClient(
        cluster_name=cluster_name,
        cluster_members=members,
    )
    counter_queue = hazelcast_client.get_queue(counter_queue_name).blocking()
    print(f"[facade-service] connected to Hazelcast queue '{counter_queue_name}' with members: {members}")


@app.on_event("startup")
async def startup():
    await asyncio.to_thread(
        register_service,
        name=SERVICE_NAME,
        service_id=SERVICE_ID,
        address=SERVICE_ADDRESS,
        port=SERVICE_PORT,
        tags=["api", "facade"],
    )
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
                    timeout=2,
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
                response = stub.GetLogs(logging_pb2.Empty(), timeout=2)
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
            "port_8500": "Consul UI and API",
            "port_50051": "Logging Service (gRPC)"
        }
    }

@app.post("/send")
async def send_msg(data: Msg):
    msg_id = str(uuid.uuid4())
    start = time.perf_counter()
    
    try:
        logging_addrs = await discover("logging-service")
        logging_start = time.perf_counter()
        status, selected_addr = await asyncio.to_thread(
            grpc_send_log_with_failover,
            logging_addrs,
            msg_id,
            data.msg,
        )
        logging_ms = round((time.perf_counter() - logging_start) * 1000, 2)

        queue_start = time.perf_counter()
        await asyncio.to_thread(
            enqueue_counter_message,
            {
                "uuid": msg_id,
                "msg": data.msg,
                "account": data.account,
                "amount": data.amount,
            },
        )
        counter_queue_ms = round((time.perf_counter() - queue_start) * 1000, 2)
        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)

        return {
            "uuid": msg_id,
            "status": status,
            "logging_instance": selected_addr,
            "counter_status": "queued",
            "elapsed_ms": elapsed_ms,
            "logging_ms": logging_ms,
            "counter_queue_ms": counter_queue_ms,
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
