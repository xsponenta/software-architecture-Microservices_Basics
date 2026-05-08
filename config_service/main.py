from collections import defaultdict
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


app = FastAPI(title="Config Server")


class ServiceRegistration(BaseModel):
    name: str
    address: str


registry: Dict[str, set[str]] = defaultdict(set)


@app.post("/register")
async def register_service(data: ServiceRegistration):
    registry[data.name].add(data.address)
    print(f"[config-server] registered {data.name}: {data.address}")
    return {"status": "registered", "service": data.name, "addresses": sorted(registry[data.name])}


@app.get("/services/{service_name}")
async def get_service(service_name: str):
    addresses: List[str] = sorted(registry.get(service_name, set()))
    if not addresses:
        raise HTTPException(status_code=404, detail=f"No instances registered for {service_name}")
    return {"service": service_name, "addresses": addresses}


@app.get("/services")
async def list_services():
    return {name: sorted(addresses) for name, addresses in registry.items()}
