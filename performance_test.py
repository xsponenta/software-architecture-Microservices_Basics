import json
import statistics
import time
from urllib import request


FACADE_URL = "http://localhost:8000"
REQUESTS_PER_SCENARIO = 100


def post_transaction(payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        f"{FACADE_URL}/send",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def run_scenario(name: str, account_count: int) -> dict:
    total_times = []
    logging_times = []
    counter_queue_times = []

    started = time.perf_counter()
    for i in range(REQUESTS_PER_SCENARIO):
        account = f"account-{i % account_count}"
        before = time.perf_counter()
        response = post_transaction(
            {
                "msg": f"{name}-{i}",
                "account": account,
                "amount": 1,
            }
        )
        total_times.append((time.perf_counter() - before) * 1000)
        logging_times.append(float(response.get("logging_ms", 0)))
        counter_queue_times.append(float(response.get("counter_queue_ms", 0)))

    return {
        "scenario": name,
        "accounts": account_count,
        "requests": REQUESTS_PER_SCENARIO,
        "wall_time_ms": round((time.perf_counter() - started) * 1000, 2),
        "avg_total_ms": round(statistics.mean(total_times), 2),
        "avg_logging_ms": round(statistics.mean(logging_times), 2),
        "avg_counter_queue_ms": round(statistics.mean(counter_queue_times), 2),
    }


def main():
    results = [
        run_scenario("10-accounts", 10),
        run_scenario("1-account", 1),
    ]

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
