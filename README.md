# Lab 5: Microservices with Consul

This branch implements the `micro_consul` task on top of Lab 4.

- Consul is used as Service Registry, Service Discovery, and Config Server.
- `facade-service`, `counter-service`, and every `logging-service` instance register themselves in Consul at startup.
- `facade-service` discovers `logging-service` and `counter-service` through Consul health checks.
- Hazelcast client settings are stored in Consul KV and read by `logging-service`.
- Message queue settings are stored in Consul KV and read by `facade-service` and `counter-service`.
- Hazelcast Queue is still used as the message queue between `facade-service` and `counter-service`.

## Run

```bash
docker compose down -v --remove-orphans
docker compose up --build -d
```

Services:

- Facade: `http://localhost:8000`
- Counter: `http://localhost:8002/messages`
- Consul UI: `http://localhost:8500`
- Consul services API: `http://localhost:8500/v1/catalog/services`

## Show Running Containers

```bash
docker compose ps
```

Expected containers:

- `consul`
- `consul-init`
- `facade-service`
- `counter-service`
- `logging-service-1`
- `logging-service-2`
- `logging-service-3`
- `hazelcast-1`
- `hazelcast-2`
- `hazelcast-3`
- `postgres`

## Show Consul Registration

```bash
curl -s http://localhost:8500/v1/catalog/services
```

```bash
curl -s "http://localhost:8500/v1/health/service/logging-service?passing=true"
curl -s "http://localhost:8500/v1/health/service/counter-service?passing=true"
curl -s "http://localhost:8500/v1/health/service/facade-service?passing=true"
```

Also open the UI:

```text
http://localhost:8500/ui
```

## Show Consul KV Config

```bash
curl -s 'http://localhost:8500/v1/kv/config/hazelcast/members?raw'
curl -s 'http://localhost:8500/v1/kv/config/hazelcast/map_name?raw'
curl -s 'http://localhost:8500/v1/kv/config/mq/hazelcast_members?raw'
curl -s 'http://localhost:8500/v1/kv/config/mq/queue_name?raw'
```

## Post 10 Transactions

```bash
for i in {1..10}; do
  curl -s -X POST http://localhost:8000/send \
    -H "Content-Type: application/json" \
    -d "{\"msg\":\"msg$i\"}"
  echo
done
```

Each response includes:

- `logging_instance`
- `counter_status: queued`
- `elapsed_ms`

## Read Through Facade

```bash
curl -s http://localhost:8000/receive
```

For `msg1..msg10`, the default account balance must be `55.0`.

## Show Logging-Service Distribution

```bash
docker logs logging-service-1 --tail 50
docker logs logging-service-2 --tail 50
docker logs logging-service-3 --tail 50
```

## Performance Test

Run two scenarios required by the task: 10 accounts and 1 account.

```bash
python3 performance_test.py
```

The script prints:

- `wall_time_ms`
- `avg_total_ms`
- `avg_logging_ms`
- `avg_counter_queue_ms`

Use these values in the report table as the Task 5 results. Since `counter-service` is asynchronous in this lab, `avg_counter_queue_ms` is the facade-side contribution for putting the transaction into the message queue.

## Failure Test With Consul

Stop one logging instance:

```bash
docker stop logging-service-1
sleep 10
```

Check Consul health:

```bash
curl -s "http://localhost:8500/v1/health/service/logging-service"
```

In the Consul UI, `logging-service-1` should become unhealthy or critical. POST requests still work because `facade-service` asks Consul for passing instances and falls back to another logging instance if one is unavailable.

```bash
curl -s -X POST http://localhost:8000/send \
  -H "Content-Type: application/json" \
  -d '{"msg":"after-logging-pause"}'
```

Start the logging instance again:

```bash
docker start logging-service-1
```

Counter queue failure test:

```bash
docker pause counter-service
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg11"}'
curl -s -X POST http://localhost:8000/send -H "Content-Type: application/json" -d '{"msg":"msg12"}'
curl -s http://localhost:8000/receive
docker unpause counter-service
sleep 3
curl -s http://localhost:8000/receive
```

While `counter-service` is paused, the `counter` part of the GET response returns `null` values. After unpause, queued messages are consumed and balances become correct again.
