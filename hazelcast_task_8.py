import hazelcast
import multiprocessing
import time

CLUSTER_NAME = "helloworld" 
QUEUE_NAME = "boundedQueue" 
TOTAL_MESSAGES = 100

def create_client():
    return hazelcast.HazelcastClient(cluster_name=CLUSTER_NAME)

def producer():
    client = create_client()
    queue = client.get_queue(QUEUE_NAME).blocking()
    
    for i in range(1, TOTAL_MESSAGES + 1):
        queue.put(i)  
        print(f"Produced: {i}")
    
    client.shutdown()

def consumer(consumer_id):
    client = create_client()
    queue = client.get_queue(QUEUE_NAME).blocking()

    while True:
        item = queue.poll(1) 
        if item is None:
            break
        print(f"Consumer {consumer_id} consumed: {item}")

    client.shutdown()

if __name__ == "__main__":
    producer_process = multiprocessing.Process(target=producer)
    consumer_process1 = multiprocessing.Process(target=consumer, args=(1,))
    consumer_process2 = multiprocessing.Process(target=consumer, args=(2,))

    #consumer_process1.start()
    #consumer_process2.start()
    producer_process.start()

    producer_process.join()
    #consumer_process1.join()
    #consumer_process2.join()
    
    print("All processes finished.")