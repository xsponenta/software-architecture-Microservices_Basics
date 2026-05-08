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

If old lab data makes screenshots noisy, start from a clean local database:

```bash
docker compose down -v --remove-orphans
docker compose up --build -d
```

## Check Service Registration

```bash
curl -s http://localhost:8003/services
```

Expected services:

- `facade-service`
- `counter-service`
- `logging-service` with three addresses

## Post 10 Transactions

```bash
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg1"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg2"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg3"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg4"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg5"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg6"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg7"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg8"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg9"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg10"}'
```

Each response includes:

- `logging_instance`: selected logging instance
- `counter_status`: `queued`
- `elapsed_ms`: facade processing time

`msg1` through `msg10` are interpreted as amounts `1..10`, so the default account balance becomes `55`.
You can also send explicit account/amount JSON:

```bash
curl -s -X POST http://localhost:8000/send \
  -H "Content-Type: application/json" \
  -d '{"msg":"deposit", "account":"acc-1", "amount":100}'
```

## Read Through Facade

```bash
curl -s http://localhost:8000/receive
```

The response contains:

- `logs`: messages read from Hazelcast Map through one discovered logging instance
- `counter.transactions`: transactions read from counter-service
- `counter.balances`: calculated balances

## Show Different Logging Instances

```bash
docker logs logging-service-1 --tail 50
docker logs logging-service-2 --tail 50
docker logs logging-service-3 --tail 50
```

Each logged message contains the instance prefix, for example:

```text
[logging-service-2] Logged message: msg5 (uuid=...)
```

## Counter Failure Test

Pause the counter service:

```bash
docker pause counter-service
```

POST still works because facade writes to Hazelcast Queue:

```bash
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg11"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg12"}'
```

GET from facade returns unavailable counter values while the counter is paused:

```bash
curl -s http://localhost:8000/receive
```

Expected counter part:

```json
{"transactions": null, "balances": null}
```

Resume the counter service:

```bash
docker unpause counter-service
```

After a few seconds, counter-service consumes the queued transactions and updates balances:

```bash
docker logs counter-service --tail 50
curl -s http://localhost:8000/receive
```
