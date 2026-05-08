# Lab 4: Microservices with Message Queue

This branch implements the `micro_mq` task:

- `facade-service` receives client `POST /send` and `GET /receive` requests.
- `logging-service` runs in three instances and stores log messages in a Hazelcast Distributed Map.
- `counter-service` stores transactions in PostgreSQL and maintains account balances.
- `facade-service` sends counter updates asynchronously through Hazelcast Queue.
- `config-server` stores service addresses registered at startup.
- `facade-service` discovers `logging-service` and `counter-service` through `config-server`.
- Hazelcast runs as a three-node cluster.

## Run

```bash
docker compose up --build -d
```

Services:

- `facade-service`: `http://localhost:8000`
- `counter-service`: `http://localhost:8002`
- `config-server`: `http://localhost:8003`
- `logging-service-1` gRPC: `localhost:50051`
- Hazelcast node 1: `localhost:5701`
- PostgreSQL: `localhost:5432`

start from a clean local database:

```bash
docker compose down -v --remove-orphans
docker compose up --build -d
```

## Check Service Registration

```bash
curl -s http://localhost:8003/services
```

services:

- `facade-service`
- `counter-service`
- `logging-service`

