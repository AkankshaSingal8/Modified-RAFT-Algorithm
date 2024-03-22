import grpc
import Raft_pb2
import Raft_pb2_grpc

class RaftNode:
    def __init__(self, node_id, nodes):
        self.node_id = node_id
        self.nodes = nodes
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_length = 0
        self.current_role = "follower"
        self.current_leader = None
        self.votes_received = set()
        self.sent_length = 0
        self.acked_length = 0

    def start(self):
        self.current_role = "follower"
        self.current_leader = None
        self.votes_received.clear()
        self.sent_length = 0
        self.acked_length = 0

    def on_leader_failure_or_timeout(self):
        self.current_term += 1
        self.current_role = "candidate"
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        last_term = 0
        if self.log:
            last_term = self.log[-1]["term"]
        msg = ("VoteRequest", self.node_id, self.current_term, len(self.log), last_term)
        for node in self.nodes:
            self.send_message(node, msg)
        # Start election timer

    def receive_vote_request(self, candidate_id, term, candidate_log_length, candidate_last_term):
        if term > self.current_term:
            self.current_term = term
            self.current_role = "follower"
            self.voted_for = None

        last_term = 0
        if self.log:
            last_term = self.log[-1]["term"]

        log_ok = (candidate_last_term > last_term) or \
                 (candidate_last_term == last_term and candidate_log_length >= len(self.log))

        if term == self.current_term and log_ok and (self.voted_for == candidate_id or self.voted_for is None):
            self.voted_for = candidate_id
            self.send_message(candidate_id, ("VoteResponse", self.node_id, self.current_term, True))
        else:
            self.send_message(candidate_id, ("VoteResponse", self.node_id, self.current_term, False))

    def receive_vote_response(self, voter_id, term, vote_granted):
        if self.current_role == "candidate" and term == self.current_term and vote_granted:
            self.votes_received.add(voter_id)
            if len(self.votes_received) >= (len(self.nodes) + 1) // 2:
                self.become_leader()
                # Cancel election timer
                for follower in self.nodes:
                    if follower != self.node_id:
                        self.sent_length[follower] = len(self.log)
                        self.acked_length[follower] = 0
                        self.replicate_log(self.node_id, follower)
        elif term > self.current_term:
            self.current_term = term
            self.current_role = "follower"
            self.voted_for = None
            # Cancel election timer

    def become_leader(self):
        self.current_role = "leader"
        self.current_leader = self.node_id
        self.votes_received.clear()
        self.sent_length = {node: len(self.log) for node in self.nodes}
        self.acked_length = {node: 0 for node in self.nodes}

    def broadcast_message(self, msg):
        if self.current_role == "leader":
            log_entry = {"msg": msg, "term": self.current_term}
            self.log.append(log_entry)
            self.acked_length[self.node_id] = len(self.log)
            for follower in self.nodes:
                if follower != self.node_id:
                    self.replicate_log(self.node_id, follower)
        else:
            # Forward the request to the current leader via a FIFO link
            pass

    def periodic_task(self):
        if self.current_role == "leader":
            for follower in self.nodes:
                if follower != self.node_id:
                    self.replicate_log(self.node_id, follower)

    def replicate_log(self, leader_id, follower_id):
        prefix_len = self.sent_length[follower_id]
        suffix = self.log[prefix_len:]

        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self.log[prefix_len - 1]["term"]

        msg = ("LogRequest", leader_id, self.current_term, prefix_len, prefix_term, self.commit_length, suffix)
        self.send_message(follower_id, msg)

    def receive_log_request(self, leader_id, term, prefix_len, prefix_term, leader_commit, suffix, sender_id):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            # Cancel election timer

        if term == self.current_term:
            self.current_role = "follower"
            self.current_leader = leader_id

        log_ok = (len(self.log) >= prefix_len) and \
                 (prefix_len == 0 or self.log[prefix_len - 1]["term"] == prefix_term)

        if term == self.current_term and log_ok:
            ack = prefix_len + len(suffix)
            self.append_entries(prefix_len, leader_commit, suffix)
            self.send_message(leader_id, ("LogResponse", self.node_id, self.current_term, ack, True))
        else:
            self.send_message(leader_id, ("LogResponse", self.node_id, self.current_term, 0, False))

    def append_entries(self, prefix_len, leader_commit, suffix):
        if suffix and len(self.log) > prefix_len:
            index = min(len(self.log), prefix_len + len(suffix)) - 1
            if self.log[index]["term"] != suffix[index - prefix_len]["term"]:
                self.log = self.log[:prefix_len]

        if prefix_len + len(suffix) > len(self.log):
            for i in range(len(self.log) - prefix_len, len(suffix)):
                self.log.append(suffix[i - (len(self.log) - prefix_len)])

        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                self.deliver_message(self.log[i]["msg"])
            self.commit_length = leader_commit

    def deliver_message(self, message):
        # Code for delivering message to the application
        pass

    def send_message(self, node, message):
        # Code for sending message to another node
        pass

    def receive_log_response(self, follower_id, term, ack, success):
        if term == self.current_term and self.current_role == "leader":
            if success and ack >= self.acked_length[follower_id]:
                self.sent_length[follower_id] = ack
                self.acked_length[follower_id] = ack
                self.commit_log_entries()
            elif self.sent_length[follower_id] > 0:
                self.sent_length[follower_id] -= 1
                self.replicate_log(self.node_id, follower_id)
        elif term > self.current_term:
            self.current_term = term
            self.current_role = "follower"
            self.voted_for = None
            # Cancel election timer

    def commit_log_entries(self):
        min_acks = (len(self.nodes) + 1) // 2
        ready = [length for length in range(1, len(self.log) + 1) if self.acks(length) >= min_acks]
        
        if ready and max(ready) > self.commit_length and self.log[max(ready) - 1]["term"] == self.current_term:
            for i in range(self.commit_length, max(ready)):
                self.deliver_message(self.log[i]["msg"])
            self.commit_length = max(ready)

    def acks(self, length):
        return sum(1 for node_id in self.nodes if self.acked_length[node_id] >= length)

    def on_heartbeat_timeout(self):
        if self.current_role == "leader":
            # Send heartbeat messages to all nodes
            pass

    def receive_heartbeat(self, leader_id, term, leader_commit, entries):
        if term < self.current_term:
            return
        self.current_term = term
        self.current_role = "follower"
        self.current_leader = leader_id
        # Process entries and update commit length
        pass

node = RaftNode(node_id=1, nodes=[2, 3, 4])  
node.start()
