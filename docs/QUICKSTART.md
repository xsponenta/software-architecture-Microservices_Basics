# Quick Start

## 1. Clone And Choose A Branch

```bash
git clone https://github.com/xsponenta/software-architecture-Microservices_Basics.git
cd software-architecture-Microservices_Basics
git checkout micro_consul
```

Use `micro_consul` for the most complete implementation. Use earlier branches if you need a specific lab stage.

## 2. Start Cleanly

When switching branches, remove old containers and volumes first:

```bash
docker compose down -v --remove-orphans
docker compose up --build -d
docker compose ps
```

## 3. Smoke Test

Send several messages:

```bash
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8000/send \
    -H "Content-Type: application/json" \
    -d "{\"msg\":\"msg$i\"}"
  echo
done
```

Read the aggregate state:

```bash
curl -s http://localhost:8000/receive
```

## 4. Consul Checks

On `micro_consul`, open:

```text
http://localhost:8500/ui
```

Or query services from the terminal:

```bash
curl -s http://localhost:8500/v1/catalog/services
curl -s "http://localhost:8500/v1/health/service/logging-service?passing=true"
curl -s "http://localhost:8500/v1/health/service/facade-service?passing=true"
```

## 5. Stop The Stack

```bash
docker compose down -v --remove-orphans
```
