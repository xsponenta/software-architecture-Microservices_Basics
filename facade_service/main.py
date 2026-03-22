import uuid
import grpc
import httpx
import asyncio
import os
import random
from fastapi import FastAPI
from pydantic import BaseModel

import logging_pb2
import logging_pb2_grpc

app = FastAPI()

LOGGING_GRPC_ADDRS = [
    address.strip()
    for address in os.getenv(
        "LOGGING_GRPC_ADDRS",
        "logging-service-1:50051,logging-service-2:50051,logging-service-3:50051",
    ).split(",")
    if address.strip()
]
MESSAGES_HTTP_URL = os.getenv("MESSAGES_HTTP_URL", "http://localhost:8002/messages")

class Msg(BaseModel):
    msg: str

def _get_randomized_grpc_addresses() -> list[str]:
    addresses = LOGGING_GRPC_ADDRS.copy()
    random.shuffle(addresses)
    return addresses


def grpc_send_log_with_failover(msg_uuid: str, text: str):
    last_error = None
    for addr in _get_randomized_grpc_addresses():
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


def grpc_get_logs_with_failover():
    last_error = None
    for addr in _get_randomized_grpc_addresses():
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
            "port_8002": "Messages Service",
            "port_50051": "Logging Service (gRPC)"
        }
    }

@app.post("/send")
async def send_msg(data: Msg):
    msg_id = str(uuid.uuid4())
    
    try:
        status, selected_addr = await asyncio.to_thread(grpc_send_log_with_failover, msg_id, data.msg)

        counter_status = "counter-unavailable"
        async with httpx.AsyncClient() as client:
            try:
                counter_resp = await client.post(
                    MESSAGES_HTTP_URL,
                    json={"uuid": msg_id, "msg": data.msg},
                    timeout=5,
                )
                counter_status = counter_resp.json().get("status", "unknown")
            except httpx.RequestError:
                counter_status = "counter-unavailable"

        return {
            "uuid": msg_id,
            "status": status,
            "logging_instance": selected_addr,
            "counter_status": counter_status,
        }
    except Exception as e:
        print(f"[ERROR] Retry failed: {e}")
        return {"uuid": msg_id, "status": "failed", "error": str(e)}

@app.get("/receive")
async def get_combined():
    try:
        logs_str, selected_addr = await asyncio.to_thread(grpc_get_logs_with_failover)
    except grpc.RpcError:
        logs_str = "<logging unavailable>"
        selected_addr = "<none>"

    async with httpx.AsyncClient() as client:
        try:
            msg_resp = await client.get(MESSAGES_HTTP_URL)
            static_msg = msg_resp.json()
        except httpx.RequestError:
            static_msg = "<messages unavailable>"

    if logs_str and logs_str != "<logging unavailable>":
        logs_list = [log.strip() for log in logs_str.split(",") if log.strip()]
        logs_formatted = "\n".join(logs_list)
    else:
        logs_formatted = ""
    
    return {
        "logging_instance": selected_addr,
        "logs": logs_formatted,
        "message": static_msg
    }