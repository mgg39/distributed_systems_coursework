import socket
import os
import threading
import time
import random
import json
from collections import defaultdict

FILES_DIR = "server_files"
NUM_FILES = 100
LOCK_LEASE_DURATION = 20  # Lease duration in seconds
STATE_FILE = os.path.join(os.path.dirname(__file__), "server_state_1.json")

# Ensure the files directory exists and create 100 files
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)
for i in range(NUM_FILES):
    open(os.path.join(FILES_DIR, f"file_{i}"), 'a').close()

class LockManagerServer:
    def __init__(self, host='localhost', port=8080, server_id=1, peers=[("localhost", 8083), ("localhost", 8085)]):
        self.server_address = (host, port)
        self.server_id = server_id
        self.peers = peers
        self.role = 'leader' if self.server_id == 1 else 'follower'  # Server 1 is initially the leader
        self.leader_address = self.server_address if self.role == 'leader' else None
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(1, 2)
        self.term = 0

        # Initialize UDP socket for client requests
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(self.server_address)
        
        # Separate UDP socket for Raft messages
        self.raft_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.raft_sock.bind((host, port + 1))
        self.raft_sock.settimeout(1)  # Non-blocking mode with timeout

        # Lock management
        self.lock = threading.Lock()
        self.current_lock_holder = None
        self.lock_expiration_time = None
        self.request_history = defaultdict(dict)

        # Load state for fault tolerance
        self.load_state()
        if not os.path.exists(STATE_FILE):
            self.save_state()

        # Start threads
        if self.role == 'leader':
            threading.Thread(target=self.send_heartbeats, daemon=True).start()
        threading.Thread(target=self.monitor_lock_expiration, daemon=True).start()
        threading.Thread(target=self.heartbeat_check, daemon=True).start()
        threading.Thread(target=self.handle_messages, daemon=True).start()

    def start(self):
        print(f"Server {self.server_id} listening on {self.server_address}")
        while True:
            data, client_address = self.sock.recvfrom(1024)  # Blocking mode for client requests
            threading.Thread(target=self.handle_request, args=(data, client_address)).start()
    
    def handle_messages(self):
        while True:
            try:
                data, addr = self.raft_sock.recvfrom(1024)
                message = data.decode()
                parts = message.split(":")

                if parts[0] == "heartbeat":
                    term = int(parts[1])
                    leader_id = int(parts[2])
                    self.process_heartbeat(term, leader_id)

                elif parts[0] == "request_vote":
                    term = int(parts[1])
                    candidate_id = int(parts[2])
                    self.process_vote_request(term, candidate_id, addr)

            except socket.timeout:
                continue
            except Exception as e:
                print(f"[ERROR] handle_messages error: {e}")
    
    def process_heartbeat(self, term, leader_id):
        if term >= self.term:
            self.term = term
            self.role = 'follower'
            self.leader_address = self.peers[leader_id - 1]
            self.last_heartbeat = time.time()
            print(f"[DEBUG] Received heartbeat from leader {leader_id}")

    def process_vote_request(self, term, candidate_id, addr):
        if term > self.term:
            self.term = term
            self.voted_for = candidate_id
            self.raft_sock.sendto("vote_granted".encode(), addr)
            self.role = 'follower'
            self.last_heartbeat = time.time()
            print(f"[DEBUG] Voted for candidate {candidate_id} for term {term}")

    def acquire_lock(self, client_id):
        if self.role == 'leader':
            with self.lock:
                current_time = time.time()
                if not self.current_lock_holder or (self.lock_expiration_time and current_time > self.lock_expiration_time):
                    self.current_lock_holder = client_id
                    self.lock_expiration_time = current_time + LOCK_LEASE_DURATION
                    self.save_state()
                    return f"grant lock:{LOCK_LEASE_DURATION}"
                else:
                    return "Lock is currently held"
        else:
            return f"Redirect to leader at {self.leader_address}"

    def release_lock(self, client_id):
        with self.lock:
            if self.current_lock_holder == client_id:
                self.current_lock_holder = None
                self.lock_expiration_time = None
                self.save_state()
                return "unlock success"
            else:
                return "You do not hold the lock"

    def renew_lease(self, client_id):
        with self.lock:
            if self.current_lock_holder == client_id:
                self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                return "lease renewed"
            else:
                return "You do not hold the lock"

    def append_to_file(self, client_id, file_name, data):
        if self.current_lock_holder == client_id:
            file_path = os.path.join(FILES_DIR, file_name)
            if os.path.exists(file_path):
                with open(file_path, 'a') as f:
                    f.write(data + "\n")
                    f.flush()
                    os.fsync(f.fileno())
                return "append success"
            else:
                return "File not found"
        else:
            return "You do not hold the lock"

    def monitor_lock_expiration(self):
        while True:
            with self.lock:
                if self.current_lock_holder and self.lock_expiration_time and time.time() > self.lock_expiration_time:
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.save_state()
            time.sleep(1)
    
    def handle_request(self, data, client_address):
        message = data.decode()
        if message.startswith("acquire_lock"):
            client_id = message.split(":")[1]
            response = self.acquire_lock(client_id)
            self.sock.sendto(response.encode(), client_address)
        
        elif message.startswith("release_lock"):
            client_id = message.split(":")[1]
            response = self.release_lock(client_id)
            self.sock.sendto(response.encode(), client_address)

        elif message.startswith("append_file"):
            client_id, file_name, file_data = message.split(":")[1:4]
            response = self.append_to_file(client_id, file_name, file_data)
            self.sock.sendto(response.encode(), client_address)

        elif message == "identify_leader":
            response = "I am the leader" if self.role == 'leader' else f"Redirect to leader at {self.leader_address}"
            self.sock.sendto(response.encode(), client_address)
        
        elif message == "ping":
            response = "pong"
            self.sock.sendto(response.encode(), client_address)

    def heartbeat_check(self):
        while True:
            if self.role == 'follower' and (time.time() - self.last_heartbeat) > self.election_timeout:
                print(f"[DEBUG] Server {self.server_id} detected leader failure, starting election")
                self.start_election()
            time.sleep(0.1)
    
    def start_election(self):
        # Only start an election if this server isn't already the leader
        if self.role == 'leader':
            return

        # Assume this server will be the leader unless a lower-ID server is found to be active
        lowest_active_peer_found = False

        # Check each lower-ID server to see if it's alive
        for peer_id, peer_address in sorted([(i+1, p) for i, p in enumerate(self.peers)]):
            if peer_id < self.server_id:
                # Ping each lower-ID peer to check if they are still alive
                if self.is_peer_alive(peer_address):
                    # If an active lower-ID server is found, this server should not be the leader
                    self.role = 'follower'
                    self.leader_address = peer_address
                    print(f"[DEBUG] Server {self.server_id} defers to active lower-ID leader: Server {peer_id}")
                    lowest_active_peer_found = True
                    break

        # If no active lower-ID servers were found, this server becomes the leader
        if not lowest_active_peer_found:
            self.role = 'leader'
            self.leader_address = self.server_address
            print(f"[DEBUG] Server {self.server_id} became the leader due to no active lower-ID servers")
            self.send_heartbeats()
        
    def is_peer_alive(self, peer_address):
        try:
            # Send a "ping" message to the peer
            self.sock.sendto("ping".encode(), peer_address)
            print(f"[DEBUG] Server {self.server_id} sent ping to {peer_address}")
            
            # Set a timeout for receiving the response
            self.sock.settimeout(1)
            
            # Wait for the response
            response, _ = self.sock.recvfrom(1024)
            
            # Check if the response is "pong"
            if response.decode() == "pong":
                print(f"[DEBUG] Server {self.server_id} received pong from {peer_address}")
                return True
        except socket.timeout:
            print(f"[DEBUG] Server {self.server_id} timed out waiting for response from {peer_address}")
        return False


    def request_vote(self, peer, term, candidate_id):
        message = f"request_vote:{term}:{candidate_id}"
        self.sock.sendto(message.encode(), peer)

    def send_heartbeats(self):
        while self.role == 'leader':
            message = f"heartbeat:{self.term}:{self.server_id}"
            for peer in self.peers:
                self.sock.sendto(message.encode(), peer)
            time.sleep(1)

    def save_state(self):
        state = {
            "current_lock_holder": self.current_lock_holder,
            "lock_expiration_time": self.lock_expiration_time
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)

    def load_state(self):
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            self.current_lock_holder = state.get("current_lock_holder")
            self.lock_expiration_time = state.get("lock_expiration_time")

if __name__ == "__main__":
    server = LockManagerServer()
    server.start()