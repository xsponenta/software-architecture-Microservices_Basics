import hazelcast
import threading
import time

def main(method_name="optimistic"):
    def increment_without_lock(client):
        distributed_map = client.get_map("my-distributed-map").blocking()
        distributed_map.put_if_absent("key", 0)
        for _ in range(10000):
            value = distributed_map.get("key")
            distributed_map.put("key", value + 1)

    def increment_with_pessimistic_lock(client):
        distributed_map = client.get_map("my-distributed-map").blocking()
        distributed_map.put_if_absent("key", 0)
        for _ in range(10000):
            distributed_map.lock("key")
            try:
                value = distributed_map.get("key")
                distributed_map.put("key", value + 1)
            finally:
                distributed_map.unlock("key")

    def implement_with_optimistic_lock(client):
        distributed_map = client.get_map("my-distributed-map").blocking()
        distributed_map.put_if_absent("key", 0)
        for _ in range(10000):
            while True:
                value = distributed_map.get("key")
                if distributed_map.replace_if_same("key", value, value + 1):
                    break
    
    methods = {
        "none": increment_without_lock,
        "pessimistic": increment_with_pessimistic_lock,
        "optimistic": implement_with_optimistic_lock
    }
    
    method = methods.get(method_name, implement_with_optimistic_lock)
    cluster_name = "helloworld"
    client1 = hazelcast.HazelcastClient(cluster_name=cluster_name)
    client2 = hazelcast.HazelcastClient(cluster_name=cluster_name)
    client3 = hazelcast.HazelcastClient(cluster_name=cluster_name)

    threads = [
        threading.Thread(target=method, args=(client1,)),
        threading.Thread(target=method, args=(client2,)),
        threading.Thread(target=method, args=(client3,))
    ]

    print("start time")
    start_time = time.time()

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    
    final_value = client1.get_map("my-distributed-map").blocking().get("key")
    
    print(f"end with {method_name} locking = {final_value}")
    print(f"Execution time: {end_time - start_time:.2f} sec")

    client1.shutdown()
    client2.shutdown()
    client3.shutdown()

if __name__ == "__main__":
    main()