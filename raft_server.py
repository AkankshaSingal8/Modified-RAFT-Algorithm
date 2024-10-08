import os
from node import Node
from node import FOLLOWER, LEADER
import sys
import grpc
from concurrent import futures
import raft_pb2
import raft_pb2_grpc
import time

n = None

class Raft(raft_pb2_grpc.RaftServicer):
    def Join(self, request, context):
        response = raft_pb2.JoinResponse(ok=True)
        return response

    def PutRequest(self, request, context):
        payload = {'act':None,'key':None,'value':None}
        payload['act'] = request.payload.act
        payload['key'] = request.payload.key
        payload['value'] = request.payload.value
        response = raft_pb2.PutReply()
        response.code = 'fail'
        if n.status == LEADER:
            result = n.handle_put(payload)
            if result:
                response.code='success'
        elif n.status == FOLLOWER:
            print(f'Redirect to Leader {n.leader}\n')
            response.payload.message = n.leader
            response.payload.act = payload['act']
            response.payload.key = payload['key']
            response.payload.value = payload['value']

        return response

    def GetRequest(self, request, context):
        payload = {'act': None, 'key': None,'value':None}
        payload['act'] = request.payload.act
        payload['key'] = request.payload.key
        payload['value'] = request.payload.value
        response = raft_pb2.GetReply()
        response.code = 'fail'
        if n.status == LEADER:
            result = n.handle_get(payload)
            if result and result['value']:
                response.code = 'success'
                response.payload.act = request.payload.act
                response.payload.key = request.payload.key
                response.payload.value = result['value']

        elif n.status == FOLLOWER:
            print(f'Redirect to Leader {n.leader}\n')
            response.payload.message = n.leader
            response.payload.act = payload['act']
            response.payload.key = payload['key']
            response.payload.value = payload['value']

        return response

    def RequestVote(self, request, context):
        term = request.term
        commitIdx = request.commitIdx
        leaderId = request.LeaderID
        staged = {'act': None, 'key': None, 'value': None}
        staged['act'] = request.staged.act
        staged['key'] = request.staged.key
        staged['value'] = request.staged.value
        choice, term = n.decide_vote(term, commitIdx, staged, leaderId)
        message = raft_pb2.VoteReply(choice=choice,term=term)
        return message

    def AppendEntries(self, request, context):
        lease_expiry = request.lease_expiry
        msg = {
            'term':request.term,
            'addr':request.addr,
            'lease_expiry': lease_expiry
        }
        if request.payload:
            msg['payload'] = {
             'act' : request.payload.act,
             'key' : request.payload.key,
             'value':request.payload.value
            }
        if request.action:
            msg['action'] = request.action
        if request.commitIdx:
            msg['commitIdx'] = request.commitIdx
        term, commitIdx = n.heartbeat_follower(msg)
        reply = raft_pb2.AEReply()
        reply.term = term
        reply.commitIdx = commitIdx
        return reply

    def SpreadLog(self, request, context):
        pass

    def SpreadCommit(self, request, context):
        pass


def GRPCserver(ip_list, my_ip, term, log_lines, uncommited_list):
    global n
    n = Node(ip_list, my_ip, term, log_lines, uncommited_list)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(Raft(), server)
    server.add_insecure_port(my_ip)
    time.sleep(5)
    server.start()
    try:
        while True:
            time.sleep(60 * 60 * 24)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        index = int(sys.argv[1])
        term = 0
        log_lines = []
        ip_list = []
        uncommited_list = []
        votedFor_line = ""
        with open("ip_list.txt") as f:
            for ip in f:
                ip_list.append(ip.strip())

        list_dir = []
        for idx in range(len(ip_list)):
            list_dir.append(f'./logs_node_{idx}')
        for directory in list_dir:
            if not os.path.exists(directory):
                os.makedirs(directory)

        my_ip = ip_list.pop(index)
        print(f'Server Number {index} ON\n')

        log_directory = f'./logs_node_{index}'
        if os.path.exists(os.path.join(log_directory, 'metadata.txt')):
            with open(os.path.join(log_directory, 'metadata.txt'), 'r') as f:
                for line in f:
                    if "votedFor" in line:
                        votedFor_line = line.strip()
                    if "log[]" in line:
                        log_lines.append(line.strip())
            if votedFor_line != "":
                term = votedFor_line.split()[2]
        if os.path.exists(os.path.join(log_directory, 'dump.txt')):
            with open(os.path.join(log_directory, 'dump.txt'), 'r') as f:
                for line in f:
                    if "uncommitedEntry" in line and line.strip() not in uncommited_list:
                        uncommited_list.append(line.strip())

        GRPCserver(ip_list, my_ip, term, log_lines, uncommited_list)
