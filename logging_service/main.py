import grpc
from concurrent import futures
import logging_pb2
import logging_pb2_grpc

class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def __init__(self):
        self.storage = {}

    def Log(self, request, context):
        if request.uuid in self.storage:
            print(f"[DEDUPLICATION] UUID {request.uuid} already exists.")
            return logging_pb2.LogResponse(status="duplicate")

        self.storage[request.uuid] = request.msg
        print(f"Logged message: {request.msg}")
        return logging_pb2.LogResponse(status="logged")

    def GetLogs(self, request, context):
        all_msgs = ", ".join(self.storage.values())
        return logging_pb2.LogsResponse(logs=all_msgs)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(), server)
    server.add_insecure_port('0.0.0.0:50051')
    print("Logging Service (gRPC) started on port 50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()