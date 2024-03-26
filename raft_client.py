import sys
import grpc
import raft_pb2
import raft_pb2_grpc

leader_ip_idx = 4

def invalid_input():
    print("""Invalid Input Format!\n
            python client.py SET ip:port 'K' 'V'\n
            OR\n
            python client.py GET ip:port 'K'""")
    input()
    exit()

def get_leader_ip():
    ip_list = []
    with open("ip_list.txt") as f:
        for ip in f:
            ip_list.append(ip.strip())
    return ip_list.pop(leader_ip_idx)

if __name__ == "__main__":
    print(' * this is grpc client')
    action = sys.argv[1]
    addr = get_leader_ip()
    key = sys.argv[2]
    while True:
        try:
            channel = grpc.insecure_channel(addr)
            if len(sys.argv) == 3:
                stub = raft_pb2_grpc.RaftStub(channel)
                response = None
                if action.lower() == 'get':
                    msg = raft_pb2.GetMessage()
                    msg.type = 'get'
                    msg.payload.act = 'get'
                    msg.payload.key = key
                    msg.payload.value = ''
                    response = stub.GetRequest(msg)
                    if response.payload.message:
                        print('redirect to leader', response.payload.message)
                        addr = response.payload.message
                    else:
                        print(response)
                        break
                else:
                    invalid_input()

            elif len(sys.argv) == 4:
                val = sys.argv[3]
                if action.lower() == 'set':
                    stub = raft_pb2_grpc.RaftStub(channel)
                    msg = raft_pb2.PutMessage()
                    msg.type = 'set'
                    msg.payload.act = 'set'
                    msg.payload.key = key
                    msg.payload.value = val
                    response = stub.PutRequest(msg)
                    if response.payload.message:
                        print('redirect to leader', response.payload.message)
                        addr = response.payload.message
                    else:
                        print(response)
                        break
                    #print(response)
                else:
                    invalid_input()
            else:
                invalid_input()

        except:
            leader_ip_idx += 1
            leader_ip_idx %= 5
            addr = get_leader_ip()
            channel = grpc.insecure_channel(addr)
