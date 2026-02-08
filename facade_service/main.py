import uuid
import grpc
import httpx
import asyncio
import os
from fastapi import FastAPI
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

import logging_pb2
import logging_pb2_grpc

app = FastAPI()

LOGGING_GRPC_ADDR = os.getenv("LOGGING_GRPC_ADDR", "localhost:50051")
MESSAGES_HTTP_URL = os.getenv("MESSAGES_HTTP_URL", "http://localhost:8002/messages")

class Msg(BaseModel):
    msg: str

@retry(
    stop=stop_after_attempt(5), 
    wait=wait_fixed(3), 
    retry=retry_if_exception_type(grpc.RpcError)
)
def grpc_send_log(msg_uuid: str, text: str):
    with grpc.insecure_channel(LOGGING_GRPC_ADDR) as channel:
        stub = logging_pb2_grpc.LoggingServiceStub(channel)
        response = stub.Log(logging_pb2.LogRequest(uuid=msg_uuid, msg=text))
        return response.status

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
        status = await asyncio.to_thread(grpc_send_log, msg_id, data.msg)
        return {"uuid": msg_id, "status": status}
    except Exception as e:
        print(f"[ERROR] Retry failed: {e}")
        return {"uuid": msg_id, "status": "failed", "error": str(e)}

@app.get("/receive")
async def get_combined():
    try:
        with grpc.insecure_channel(LOGGING_GRPC_ADDR) as channel:
            stub = logging_pb2_grpc.LoggingServiceStub(channel)
            logs_res = stub.GetLogs(logging_pb2.Empty())
            logs_str = logs_res.logs 
    except grpc.RpcError:
        logs_str = "<logging unavailable>"

    async with httpx.AsyncClient() as client:
        try:
            msg_resp = await client.get(MESSAGES_HTTP_URL)
            static_msg = msg_resp.text
        except httpx.RequestError:
            static_msg = "<messages unavailable>"

    if logs_str and logs_str != "<logging unavailable>":
        logs_list = [log.strip() for log in logs_str.split(",") if log.strip()]
        logs_formatted = "\n".join(logs_list)
    else:
        logs_formatted = ""
    
    return {
        "logs": logs_formatted,
        "message": static_msg
    }