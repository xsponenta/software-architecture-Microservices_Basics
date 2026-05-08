import base64
import os
import random
import time
from typing import Any

import httpx


CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")


def wait_for_consul(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            response = httpx.get(f"{CONSUL_URL}/v1/status/leader", timeout=2)
            response.raise_for_status()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(1)

    raise RuntimeError(f"Consul is unavailable: {last_error}")


def get_kv(key: str, default: str | None = None, timeout_seconds: int = 60) -> str:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            response = httpx.get(f"{CONSUL_URL}/v1/kv/{key}", timeout=2)
            if response.status_code == 404:
                if default is not None:
                    return default
                time.sleep(1)
                continue
            response.raise_for_status()
            value = response.json()[0]["Value"]
            if value is None:
                return ""
            return base64.b64decode(value).decode("utf-8")
        except Exception as exc:
            last_error = exc
            time.sleep(1)

    if default is not None:
        return default
    raise RuntimeError(f"Consul KV key '{key}' is unavailable: {last_error}")


def get_csv_kv(key: str, default: str | None = None) -> list[str]:
    raw = get_kv(key, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def register_service(
    *,
    name: str,
    service_id: str,
    address: str,
    port: int,
    tags: list[str] | None = None,
) -> None:
    wait_for_consul()
    payload: dict[str, Any] = {
        "ID": service_id,
        "Name": name,
        "Address": address,
        "Port": port,
        "Tags": tags or [],
        "Check": {
            "TCP": f"{address}:{port}",
            "Interval": "5s",
            "Timeout": "2s",
            "DeregisterCriticalServiceAfter": "2m",
        },
    }
    response = httpx.put(f"{CONSUL_URL}/v1/agent/service/register", json=payload, timeout=5)
    response.raise_for_status()
    print(f"[consul] registered {name} id={service_id} address={address}:{port}")


async def discover_service(service_name: str, *, only_passing: bool = True) -> list[str]:
    query = "?passing=true" if only_passing else ""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{CONSUL_URL}/v1/health/service/{service_name}{query}", timeout=3)
        response.raise_for_status()
        addresses = []
        for entry in response.json():
            service = entry["Service"]
            address = service.get("Address") or entry["Node"]["Address"]
            port = service["Port"]
            addresses.append(f"{address}:{port}")

    random.shuffle(addresses)
    return addresses
