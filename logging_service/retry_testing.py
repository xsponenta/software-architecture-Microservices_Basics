import grpc
import random
from concurrent import futures
import logging_pb2
import logging_pb2_grpc

class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def __init__(self, failure_probability=0.5):
        self.storage = {}
        self.failure_probability = failure_probability

    def Log(self, request, context):
        if random.random() < self.failure_probability:
            print(f"[RETRY TEST] Simulating failure for UUID: {request.uuid}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Service temporarily unavailable for retry testing")
            return logging_pb2.LogResponse()
        
        if request.uuid in self.storage:
            print(f"[DEDUPLICATION] UUID {request.uuid} already exists.")
            return logging_pb2.LogResponse(status="duplicate")

        self.storage[request.uuid] = request.msg
        print(f"Logged message: {request.msg}")
        return logging_pb2.LogResponse(status="logged")


    def GetLogs(self, request, context):
        all_msgs = ", ".join(self.storage.values())
        return logging_pb2.LogsResponse(logs=all_msgs)

def serve(failure_probability=0.5):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(
        LoggingService(failure_probability=failure_probability), 
        server
    )
    server.add_insecure_port('0.0.0.0:50051')
    print(f"Logging Service (gRPC) started on port 50051")
    print(f"Failure probability: {failure_probability * 100}%")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    import sys
    failure_prob = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
    serve(failure_probability=failure_prob)
