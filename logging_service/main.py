import grpc
import os
from concurrent import futures
import hazelcast
import logging_pb2
import logging_pb2_grpc

class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def __init__(self):
        cluster_name = os.getenv("HAZELCAST_CLUSTER_NAME", "dev")
        members_raw = os.getenv(
            "HAZELCAST_MEMBERS",
            "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
        )
        members = [member.strip() for member in members_raw.split(",") if member.strip()]
        self.instance_id = os.getenv("INSTANCE_ID", "logging-service")

        self.client = hazelcast.HazelcastClient(
            cluster_name=cluster_name,
            cluster_members=members,
        )
        map_name = os.getenv("HAZELCAST_MAP_NAME", "transactions")
        self.storage = self.client.get_map(map_name).blocking()
        print(f"[{self.instance_id}] Connected to Hazelcast cluster '{cluster_name}' with members: {members}")

    def Log(self, request, context):
        if self.storage.contains_key(request.uuid):
            print(f"[{self.instance_id}] [DEDUPLICATION] UUID {request.uuid} already exists.")
            return logging_pb2.LogResponse(status="duplicate")

        self.storage.put(request.uuid, request.msg)
        print(f"[{self.instance_id}] Logged message: {request.msg} (uuid={request.uuid})")
        return logging_pb2.LogResponse(status="logged")

    def GetLogs(self, request, context):
        entries = self.storage.entry_set()
        all_msgs = ", ".join(value for _, value in entries)
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