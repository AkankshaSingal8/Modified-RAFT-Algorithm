import os
import threading
import time
import grpc
import raft_pb2
import raft_pb2_grpc
import random
import requests
import sys

FOLLOWER = 0
CANDIDATE = 1
LEADER = 2
addr = None
LOW_TIMEOUT = 5000
HIGH_TIMEOUT = 10000
HB_TIME = 50
MAX_LOG_WAIT = 1000
LEASE_TIME = 10000

def random_timeout():
    return random.randrange(LOW_TIMEOUT, HIGH_TIMEOUT) / 1000

def write_to_log(context, log_dir):
    with open(os.path.join(log_dir, 'logs.txt'), 'a+') as f:
        f.write(context)

def write_to_metadata(context, log_dir):
    with open(os.path.join(log_dir, 'metadata.txt'), 'a+') as f:
        f.write(context)

def write_to_dump(context, log_dir):
    with open(os.path.join(log_dir, 'dump.txt'), 'a+') as f:
        f.write(context)


class Cache:
    def __init__(self):
        self.mapping = {}
        self.order = []

    def set(self, key, value):
        if key in self.mapping:
            self.order.remove(key)
        self.mapping[key] = value
        self.order.append(key)

    def printcache(self):
        if self.mapping:
            print('Current Cache:', end=' ')
            for key, value in self.mapping.items():
                print(f'<{key}, {value}>', end=', ')
            print('\n')

    def get(self, key):
        if key in self.mapping:
            self.order.remove(key)
            self.order.append(key)
            return self.mapping[key]
        else:
            return None


class Node():
    def __init__(self, fellow, my_ip, term=0, log_list=[], uncommited_list=[]):
        self.addr = my_ip
        self.fellow = fellow
        self.lock = threading.Lock()
        self.DB = {}
        self.log = []
        self.staged = None
        self.term = int(term)
        self.status = FOLLOWER
        self.majority = len(self.fellow) // 2 + 1
        self.voteCount = 0
        self.commitIdx = 0
        self.timeout_thread = None
        self.init_timeout()
        self.cache = Cache()
        self.vote_requests_sent = set()
        self.log_dir = f'./logs_node_{self.addr[-1]}'
        self.uncommited_list = uncommited_list
        self.load_from_log(log_list, uncommited_list)
        self.lease_expiry = 0
        self.heartbeat_recieved = [False] * len(self.fellow)
        self.lease_expiry_list = [False] * len(self.fellow)

    def onServers(self):
        count = 0
        for each in self.fellow:
            channel = grpc.insecure_channel(each)
            stub = raft_pb2_grpc.RaftStub(channel)
            ping = raft_pb2.JoinRequest()
            try:
                response = stub.Join(ping)
                count += 1
            except:
                continue
        return count

    def log_contains_entry(self, entry):
        log_file_path = os.path.join(self.log_dir, 'logs.txt')
        if not os.path.exists(log_file_path):
            return False
        
        with open(log_file_path, 'r') as f:
            for line in f:
                if line.strip() == entry.strip():
                    return True
        return False

    def load_from_log(self, log_list, uncommited_list):
        for i in log_list:
            key = i.split()[-2]
            value = i.split()[-1]
            self.cache.set(key, value)
        for i in range(len(uncommited_list)):
            key = uncommited_list[i].split()[-2]
            value = uncommited_list[i].split()[-1]
            self.cache.set(key, value)
            log_dir = f'./logs_node_{self.addr[-1]}'
            write_to_log(f"SET {key} {value} {self.term}\n", log_dir)
            write_to_metadata(f'log[] - {self.term} SET {key} {value}\n', log_dir)

    def acquire_lease(self):
        while True:
            if sum(self.lease_expiry_list) == 0:
                self.lease_expiry = time.time() + LEASE_TIME / 1000
                self.lease_expiry_list[int(self.addr[-1])] = True
                print(f'Acquired lease with duration {LEASE_TIME / 1000} s\n')
                return True
            else:
                write_to_dump(f'New Leader waiting for Old Leader Lease to timeout.\n', self.log_dir)
                print('New Leader waiting for Old Leader Lease to timeout\n')
                self.lease_expiry_list = [False] * len(self.fellow)
                print('Old Leader Lease timeout\n')

    def incrementVote(self, term):
        self.voteCount += 1
        if self.status == CANDIDATE and self.term == term and self.voteCount >= self.majority and self.onServers() + 1 >= self.majority and self.acquire_lease():
            log_entry = f'NO-OP {self.term}\n'
            if not self.log_contains_entry(log_entry):
                print(f'Server {self.addr[-1]} becomes LEADER of term {self.term}\n')
                log_entry = f'NO-OP {self.term}\n'
                write_to_dump(f'Node {self.addr[-1]} became the leader for term {self.term}.\n', self.log_dir)
                for node in self.fellow:
                    log_dir = f'./logs_node_{node[-1]}'
                    write_to_log(log_entry, log_dir)
                self.status = LEADER
                self.startHeartBeat()

    def startElection(self):
        write_to_dump(f'Node {self.addr[-1]} election timer timed out, Starting election.\n', self.log_dir)
        self.term += 1
        self.voteCount = 0
        self.status = CANDIDATE
        self.init_timeout()
        print(self.log_dir)
        write_to_dump(f'Vote granted for Node {self.addr[-1]} in term {self.term}\n', self.log_dir)
        write_to_metadata(f'votedFor - {self.term} {self.addr[-1]}\n', self.log_dir)
        self.incrementVote(self.term)
        self.vote_requests_sent = set()
        self.send_vote_req()

    def send_vote_req(self):
        for voter in self.fellow:
            try:
                threading.Thread(target=self.ask_for_vote, args=(voter, self.term)).start()
            except:
                continue

    def ask_for_vote(self, voter, term):
        channel = grpc.insecure_channel(voter)
        stub = raft_pb2_grpc.RaftStub(channel)
        message = raft_pb2.VoteMessage()
        message.term = term
        message.commitIdx = self.commitIdx
        message.LeaderID = self.addr[-1]
        if self.staged:
            message.staged.act = self.staged['act']
            message.staged.key = self.staged['key']
            message.staged.value = self.staged['value']

        while self.status == CANDIDATE and self.term == term and (voter not in self.vote_requests_sent):
            try:
                reply = stub.RequestVote(message)
                self.vote_requests_sent.add(voter)
                if reply:
                    choice = reply.choice
                    if choice and self.status == CANDIDATE:
                        # log_dir = f'./logs_node_{voter[-1]}'
                        # print(log_dir)
                        # write_to_metadata(f'votedFor - {term} {self.addr[-1]}\n', log_dir)
                        # write_to_dump(f'Vote granted for Node {self.addr[-1]} in term {term}\n', log_dir)
                        self.incrementVote(term)
                    elif not choice:
                        term = int(reply.term)
                        if term > self.term:
                            self.term = term
                            self.status = FOLLOWER
                        # log_dir = f'./logs_node_{voter[-1]}'
                        # write_to_dump(f'Vote declined for Node {self.addr[-1]} in term {term}\n', log_dir)    
                    break
            except:
                continue

    def decide_vote(self, term, commitIdx, staged, leaderId):
        if self.term < term and self.commitIdx <= commitIdx and (
                staged or (self.staged == staged)):
            self.reset_timeout()
            self.term = term
            write_to_metadata(f'votedFor - {term} {leaderId}\n', self.log_dir)
            write_to_dump(f'Vote granted for Node {leaderId} in term {term}\n', self.log_dir)
            return True, self.term
        else:
            write_to_dump(f'Vote declined for Node {leaderId} in term {term}\n', self.log_dir)
            return False, self.term

    def startHeartBeat(self):
        write_to_dump(f'Leader {self.addr[-1]} sending heartbeat & Renewing Lease\n', self.log_dir)
        if self.staged:
            self.handle_put(self.staged)
        self.heartbeat_recieved = [False] * len(self.fellow)
        self.heartbeat_recieved[int(self.addr[-1])] = True
        for each in self.fellow:
            try:
                threading.Thread(target=self.send_heartbeat, args=(each, self.heartbeat_recieved, )).start()
            except:
                continue

    def update_follower_commitIdx(self, follower):
        try:
            channel = grpc.insecure_channel(follower)
            stub = raft_pb2_grpc.RaftStub(channel)
            message = raft_pb2.AEMessage()
            message.term = self.term
            message.addr = self.addr
            message.action = 'commit'
            message.payload.act = self.log[-1]['act']
            message.payload.key = self.log[-1]['key']
            message.commitIdx = self.commitIdx
            if self.log[-1]['value']:
                message.payload.value = self.log[-1]['value']
            reply = stub.AppendEntries(message)
        except:
            return        

    def send_heartbeat(self, follower, heartbeat_recieved):
        try:
            if self.log:
                self.update_follower_commitIdx(follower)
            while self.status == LEADER:
                if time.time() > self.lease_expiry:
                    self.status = FOLLOWER
                    self.lease_expiry_list = [False] * len(self.fellow)
                    self.init_timeout()
                    write_to_dump(f'Leader {self.addr[-1]} lease renewal failed. Stepping Down.\n', self.log_dir)
                    print(f'Lease expired. Stepping down\n')
                    return
                try:
                    start = time.time()
                    channel = grpc.insecure_channel(follower)
                    stub = raft_pb2_grpc.RaftStub(channel)
                    ping = raft_pb2.JoinRequest()
                    if ping:
                        try:
                            if follower not in self.fellow:
                                self.fellow.append(follower)
                            message = raft_pb2.AEMessage()
                            message.term = self.term
                            message.addr = self.addr
                            message.lease_expiry = int(self.lease_expiry)
                            reply = stub.AppendEntries(message)
                            if reply:
                                heartbeat_recieved[int(follower[-1])] = True
                                self.heartbeat_reply_handler(reply.term, reply.commitIdx)
                            delta = time.time() - start
                            time.sleep((HB_TIME - delta) / 1000)
                            if sum(heartbeat_recieved) >= self.majority:
                                if self.lease_expiry_list[int(self.addr[-1])] == True:
                                    self.lease_expiry = time.time() + LEASE_TIME / 1000
                        except:
                            heartbeat_recieved[int(follower[-1])] = False
                            write_to_dump(f'Error occurred while sending RPC to Node {follower[-1]}.\n', self.log_dir)
                            continue
                except:
                    continue
        except:
            return

    def heartbeat_reply_handler(self, term, commitIdx):
        if term > self.term:
            self.term = term
            self.status = FOLLOWER
            write_to_dump(f'Leader {self.addr[-1]} Stepping Down.\n', self.log_dir)
            self.init_timeout()

    def reset_timeout(self):
        self.election_time = time.time() + random_timeout()

    def heartbeat_follower(self, msg):
        term = msg["term"]
        if self.term <= term:
            self.leader = msg["addr"]
            received_lease_expiry = msg['lease_expiry']
            self.lease_expiry = max(self.lease_expiry, received_lease_expiry)
            self.reset_timeout()
            if self.status == CANDIDATE:
                self.status = FOLLOWER
            elif self.status == LEADER:
                self.status = FOLLOWER
                write_to_dump(f'Leader {self.addr[-1]} Stepping Down.\n', self.log_dir)
                self.init_timeout()
            if self.term < term:
                self.term = term
            if "action" in msg:
                print(f'Received request: {msg}\n')
                action = msg["action"]
                if action == "log":
                    payload = msg["payload"]
                    self.staged = payload
                elif self.commitIdx <= msg["commitIdx"]:
                    if not self.staged:
                        self.staged = msg["payload"]
                    write_to_dump(f'Node {self.addr[-1]} (follower) committed the entry SET {self.staged["key"]} {self.staged["value"]} to the state machine.\n', log_dir=f'./logs_node_{self.addr[-1]}')
                    self.commit()
        return self.term, self.commitIdx

    def init_timeout(self):
        self.reset_timeout()
        if self.timeout_thread and self.timeout_thread.is_alive():
            return
        self.timeout_thread = threading.Thread(target=self.timeout_loop)
        self.timeout_thread.start()

    def timeout_loop(self):
        while self.status != LEADER:
            delta = self.election_time - time.time()
            if delta < 0:
                self.startElection()
            else:
                time.sleep(delta)

    def handle_get(self, payload):
        key = payload["key"]
        act = payload["act"]
        if act == 'get':
            cache_res = self.cache.get(key)
            if cache_res is not None:
                print('Given result from cache to client\n')
                payload["value"] = cache_res
                return payload
            elif key in self.DB:
                print('Given result from DB to client\n')
                payload["value"] = self.DB[key]
                return payload
        return None

    def spread_update(self, message, confirmations=None, lock=None):
        for i, each in enumerate(self.fellow):
            try:
                channel = grpc.insecure_channel(each)
                stub = raft_pb2_grpc.RaftStub(channel)
                m = raft_pb2.AEMessage()
                m.term = message['term']
                m.addr = message['addr']
                if message['payload'] is not None:
                    m.payload.act = message['payload']['act']
                    m.payload.key = message['payload']['key']
                    m.payload.value = message['payload']['value']
                if message['action']:
                    m.action = message['action']
                m.commitIdx = self.commitIdx
                r = stub.AppendEntries(m)
                if r and confirmations:
                    confirmations[i] = True
                else:
                    write_to_dump(f"Node {i} rejected AppendEntries RPC from {self.addr[-1]}\n", log_dir=f'./logs_node_{i}')
            except:
                write_to_dump(f"uncommitedEntry - {self.commitIdx} SET {m.payload.key} {m.payload.value}\n", log_dir=f'./logs_node_{i}')
                continue
        if lock:
            lock.release()

    def handle_put(self, payload):
        self.lock.acquire()
        self.staged = payload
        waited = 0
        log_message = {
            "term": self.term,
            "addr": self.addr,
            "payload": payload,
            "action": "log",
            "commitIdx": self.commitIdx
        }

        write_to_dump(f"Node {self.addr[-1]} (leader) received a SET {payload['key']} {payload['value']} request.\n", self.log_dir)
        log_confirmations = [False] * len(self.fellow)
        threading.Thread(target=self.spread_update,
                         args=(log_message, log_confirmations)).start()
        while sum(log_confirmations) + 1 < self.majority:
            waited += 0.0010
            time.sleep(0.0005)
            if waited > MAX_LOG_WAIT / 1000:
                print(f'Waited for {MAX_LOG_WAIT} ms, update rejected\n')
                self.lock.release()
                return False
        commit_message = {
            "term": self.term,
            "addr": self.addr,
            "payload": payload,
            "action": "commit",
            "commitIdx": self.commitIdx
        }
        can_delete = self.commit()
        threading.Thread(target=self.spread_update,
                         args=(commit_message, None, self.lock)).start()
        print(f'Majority reached, Replied to client, Sending message to commit, Commit message: {commit_message}\n')
        write_to_dump(f"Node {self.addr[-1]} (leader) committed the entry SET {payload['key']} {payload['value']} to the state machine.\n", self.log_dir)
        write_to_log(f"SET {payload['key']} {payload['value']} {self.term}\n", self.log_dir)
        write_to_metadata(f"log[] - {self.term} SET {payload['key']} {payload['value']}\n", self.log_dir)
        return can_delete

    def commit(self):
        self.commitIdx += 1
        self.log.append(self.staged)
        key = self.staged["key"]
        act = self.staged["act"]
        value = None
        can_delete = True
        cache_update = False
        if act == 'set':
            value = self.staged["value"]
            self.DB[key] = value
            cache_update = True
        if cache_update:
            self.cache.set(key, value)
            self.cache.printcache()
        self.staged = None
        log_dir = f'./logs_node_{self.addr[-1]}'
        write_to_log(f"SET {key} {value} {self.term}\n", log_dir)
        write_to_metadata(f'log[] - {self.term} SET {key} {value}\n', log_dir)
        return can_delete
