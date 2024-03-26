import sys
import grpc
import raft_pb2
import raft_pb2_grpc

leader_ip_idx = 0 

def load_ip_list():
    """Loads the list of IP addresses from a file."""
    with open("ip_list.txt") as f:
        return [ip.strip() for ip in f]

def invalid_input():
    """Prints an error message for invalid input and exits."""
    print("Invalid Input Format!\n"
          "Use:\n"
          "  SET 'K' 'V'\n"
          "  OR\n"
          "  GET 'K'")
    sys.exit()

def get_next_leader_ip(ip_list, current_idx):
    """Calculates the next IP in the list to connect to."""
    return ip_list[(current_idx + 1) % len(ip_list)], (current_idx + 1) % len(ip_list)

def find_leader_ip(leader_id, ip_list):
    """Finds the leader's IP address given its ID."""
    # Assuming leader_id can be directly mapped to the IP. Adjust this method as per your actual leader_id to IP mapping logic.
    return ip_list.index(leader_id)

def send_request(action, key, val, ip_list):
    global leader_ip_idx
    """Sends a request to the Raft cluster, handling redirections and retries."""
    
    while True:
        addr = ip_list[leader_ip_idx]
        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.RaftStub(channel)
            print(f' * Attempting to connect to {addr}')

            if action.lower() == 'get':
                request = raft_pb2.GetMessage()
                request.type = 'get'
                request.payload.act = 'get'
                request.payload.key = key
                response = stub.GetRequest(request)
            elif action.lower() == 'set':
                request = raft_pb2.PutMessage()
                request.type = 'set'
                request.payload.act = 'set'
                request.payload.key = key
                request.payload.value = val
                response = stub.PutRequest(request)
            else:
                invalid_input()
            print('response contents', response.code, response.payload.message, response.payload.act, response.payload.key, response.payload.value)
            # Check response code for success or need to redirect to leader
            if response.code == 'success':
                print("Response:", response)
                break  # Success, exit loop
            elif response.code == 'fail' and response.payload:  # Assuming response includes leader info on failure
                try:
                    leader_addr = response.payload.message
                    print(leader_addr)
                    leader_ip_idx = int(leader_addr[-1])
                    print(leader_ip_idx)
                    print(f"Redirecting to leader at {ip_list[leader_ip_idx]}")
                except:
                    print("Invalid leader info in response:", response)
                    break
            else:
                print("Operation failed:", response)
                break  # Fail without leader info, exit loop

        except grpc.RpcError as e:
            print(f"Failed to connect to {addr}: {e}")
            # Move to the next server in the list if there's a connection issue
            _, leader_ip_idx = get_next_leader_ip(ip_list, leader_ip_idx)

if __name__ == "__main__":
    ip_list = load_ip_list()
    
    while True:
        user_input = input("Enter command (or 'exit' to quit): ")
        if user_input.lower() == 'exit':
            break

        parts = user_input.split()
        if len(parts) == 2 and parts[0].lower() == 'get':
            action, key = parts
            val = None
        elif len(parts) == 3 and parts[0].lower() == 'set':
            action, key, val = parts
        else:
            invalid_input()
            continue
        
        send_request(action, key, val, ip_list)
