# Microservices Basics

Software Architecture coursework repository for building a Python microservices system step by step. The project evolves across branches: from a basic facade/logging/messages setup to Hazelcast-backed replication, asynchronous queues, and Consul-based service discovery/configuration.

## Repository Structure

This repository intentionally uses branches as lab milestones. The `main` branch is the index and documentation hub; implementation code lives on the lab branches.

| Branch | Focus | Main additions |
| --- | --- | --- |
| [`micro_basics`](https://github.com/xsponenta/software-architecture-Microservices_Basics/tree/micro_basics) | Basic microservices | Facade service, logging service over gRPC, messages service, Docker Compose |
| [`micro_hazelcast`](https://github.com/xsponenta/software-architecture-Microservices_Basics/tree/micro_hazelcast) | Distributed logging storage | 3 logging-service instances, Hazelcast distributed map, PostgreSQL persistence |
| [`micro_mq`](https://github.com/xsponenta/software-architecture-Microservices_Basics/tree/micro_mq) | Async message queue | Config service, counter service, Hazelcast queue, async transaction updates |
| [`micro_consul`](https://github.com/xsponenta/software-architecture-Microservices_Basics/tree/micro_consul) | Service discovery and config | Consul registry, health checks, KV config, performance/failure tests |
| [`hazelcast`](https://github.com/xsponenta/software-architecture-Microservices_Basics/tree/hazelcast) | Hazelcast experiment branch | Earlier Hazelcast-focused iteration |

## System Overview

The lab series builds a small transaction/message platform:

```text
client
  -> facade-service
      -> logging-service instances over gRPC
      -> messages/counter service over HTTP or queue
      -> config service or Consul for discovery

storage:
  -> PostgreSQL for persistent records
  -> Hazelcast map / queue for distributed state and async delivery
```

The final branches demonstrate:

- service decomposition with separate HTTP/gRPC services;
- Docker Compose orchestration;
- gRPC protobuf generation;
- replicated logging with Hazelcast;
- asynchronous command delivery through a queue;
- service registration and discovery;
- configuration via Consul KV;
- resilience checks when service instances are stopped or paused.

## Quick Start

Clone the repository and switch to the lab branch you want to run:

```bash
git clone https://github.com/xsponenta/software-architecture-Microservices_Basics.git
cd software-architecture-Microservices_Basics
git checkout micro_consul
```

Start the full stack for the selected branch:

```bash
docker compose down -v --remove-orphans
docker compose up --build -d
```

Common endpoints on the advanced branches:

| Service | URL |
| --- | --- |
| Facade | `http://localhost:8000` |
| Messages / Counter | `http://localhost:8002` |
| Config service | `http://localhost:8003` |
| Consul UI | `http://localhost:8500` |
| PostgreSQL | `localhost:5432` |
| Hazelcast | `localhost:5701` |

Send a message/transaction through the facade:

```bash
curl -s -X POST http://localhost:8000/send \
  -H "Content-Type: application/json" \
  -d '{"msg":"hello"}'
```

Read the aggregated system state:

```bash
curl -s http://localhost:8000/receive
```

## Documentation

- [`docs/BRANCHES.md`](docs/BRANCHES.md) explains how the branch-based lab structure is organized.
- [`docs/QUICKSTART.md`](docs/QUICKSTART.md) gives branch checkout and smoke-test commands.
- Each implementation branch also contains its own `README.md` with branch-specific run instructions.

## Tech Stack

- Python
- FastAPI / HTTP services
- gRPC and protobuf
- Docker Compose
- PostgreSQL
- Hazelcast
- Consul

## Notes

- Generated protobuf files are committed in the lab branches for convenience.
- Use `docker compose down -v --remove-orphans` when switching branches to avoid stale containers and volumes.
- The branch names mirror the course/lab progression, so keeping them separate makes grading and comparison easier.
