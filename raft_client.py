import grpc
import Raft_pb2
import Raft_pb2_grpc

class RaftClient:
    def __init__(self, host, port):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = Raft_pb2_grpc.RaftStub(self.channel)

    def append_entries(self, request):
        response = self.stub.AppendEntries(request)
        return response

    def request_vote(self, request):
        response = self.stub.RequestVote(request)
        return response

client = RaftClient(host='localhost', port=50051)
append_entries_request = Raft_pb2.AppendEntriesRequest(...)
response = client.append_entries(append_entries_request)
print("Received response:", response)
