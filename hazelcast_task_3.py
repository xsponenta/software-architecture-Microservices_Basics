import hazelcast
import time

def main():
    print("1")
    client = hazelcast.HazelcastClient(
        cluster_name="helloworld",
        cluster_members=[
            "127.0.0.1:5701",
            "127.0.0.1:5702",
            "127.0.0.1:5703"
        ]
    )
    print("2")
    distributed_map = client.get_map("distributed-map").blocking()

    print("write 1000 values")
    for i in range(1000):
        distributed_map.put(i, f"value-{i}")
        if i % 100 == 0:
            print(f"written {i} values...")

    print(f"Done! Map size: {distributed_map.size()}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.shutdown()

if __name__ == "__main__":
    main()