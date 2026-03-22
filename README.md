# Lab 3: Microservices with Hazelcast Distributed Map

- `logging-service` runs in 3 instances (`logging-service-1..3`)
- logs are stored in Hazelcast Distributed Map (`transactions`)
- Hazelcast cluster runs with 3 nodes (`hazelcast-1..3`)
- `messages-service` uses PostgreSQL as persistent storage
- `facade-service` randomly selects a logging instance for gRPC requests
- if the selected instance is unavailable, `facade-service` automatically tries the next one

## Run

```bash
docker compose up --build
```

Services:

- `facade-service`: `http://localhost:8000`
- `messages-service`: `http://localhost:8002`
- `logging-service-1` gRPC: `localhost:50051`
- PostgreSQL: `localhost:5432`
- Hazelcast Node 1: `localhost:5701`

## Post 10 transactions

```bash
for i in {1..10}; do
    curl -s -X POST http://localhost:8000/send \
        -H "Content-Type: application/json" \
        -d "{\"msg\":\"msg$i\"}"
    echo
done
```

The response includes the `logging_instance` field that shows which instance was selected.

## Get

```bash
curl -s http://localhost:8000/receive | jq
```

Returns:

- `logs` — transactions from Hazelcast via logging-service
- `message.messages` — transactions from PostgreSQL (messages-service)

## Logs of each logging-service instance

```bash
docker logs logging-service-1 --tail 100
docker logs logging-service-2 --tail 100
docker logs logging-service-3 --tail 100
```

Each log line contains the instance prefix, for example:

`[logging-service-2] Logged message: msg4 (uuid=...)`

## Resilience testing

### 1) Stop 1-2 logging-service instances

```bash
docker stop logging-service-1
# or
docker stop logging-service-1 logging-service-2
```

Verify that POST/GET through `facade-service` still works:

```bash
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"after-stop-logging"}'
curl -s http://localhost:8000/receive | jq
```

### 2) Stop 1-2 Hazelcast nodes

```bash
docker stop hazelcast-1
# or
docker stop hazelcast-1 hazelcast-2
```

Repeat POST/GET and record the result.

