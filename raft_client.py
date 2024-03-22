import sys
import grpc
import mykvserver_pb2
import mykvserver_pb2_grpc

def invalid_input():
    print("""Invalid Input Format!\n
            python client.py PUT ip:port 'key' 'value'\n
            OR\n
            python client.py GET ip:port 'key'""")
    input()
    exit()

if __name__ == "__main__":
    print(' * this is grpc client')
    action = sys.argv[1]
    addr = sys.argv[2]
    key = sys.argv[3]
    while True:
        channel = grpc.insecure_channel(addr)
        if len(sys.argv) == 4:
            stub = mykvserver_pb2_grpc.KVServerStub(channel)
            response = None
            if action=='get' or action=='GET':
                msg = mykvserver_pb2.GetMessage()
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

        elif len(sys.argv) == 5:
            val = sys.argv[4]
            if action=='put'or action=='PUT':
                stub = mykvserver_pb2_grpc.KVServerStub(channel)
                msg = mykvserver_pb2.PutMessage()
                msg.type = 'put'
                msg.payload.act = 'put'
                msg.payload.key = key
                msg.payload.value = val
                response = stub.PutRequest(msg)
                if response.payload.message:
                    print('redirect to leader',response.payload.message)
                    addr = response.payload.message
                else:
                    print(response)
                    break
                #print(response)
            else:
                invalid_input()
        else:
            invalid_input()
