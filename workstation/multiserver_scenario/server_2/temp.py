
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
STATE_FILE = os.path.join(os.path.dirname(__file__), "server_state_2.json")

# Ensure the files directory exists and create 100 files
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)
for i in range(NUM_FILES):
    open(os.path.join(FILES_DIR, f"file_{i}"), 'a').close()

class LockManagerServer:
    def __init__(self, host='localhost', port=8082, server_id=2, peers=[("localhost", 8081), ("localhost", 8085)]):
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
                
                # Check if message starts with known non-JSON types
                if message.startswith("heartbeat") or message.startswith("request_vote"):
                    parts = message.split(":")
                    if parts[0] == "heartbeat":
                        term = int(parts[1])
                        leader_id = int(parts[2])
                        self.process_heartbeat(term, leader_id)
                    elif parts[0] == "request_vote":
                        term = int(parts[1])
                        candidate_id = int(parts[2])
                        self.process_vote_request(term, candidate_id, addr)
                    continue
                
                # Process JSON messages
                json_message = json.loads(message)
                if json_message["action"] == "replicate":
                    self.handle_replicate(json_message["file"], json_message["data"])
                elif json_message["action"] == "recover":
                    self.handle_replicate(json_message["file"], json_message["data"])

            except json.JSONDecodeError as e:
                print(f"[ERROR] JSON decode error: {e}. Received data: {data}")
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[ERROR] Unexpected error in handle_messages: {e}")

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
                print(f"[DEBUG] {client_id} appended to {file_name}: '{data}'")
                self.replicate_to_replicas(file_name, data)  # Replicate changes to other servers
                return "append success"
            else:
                return "File not found"
        else:
            return "You do not hold the lock"
    
    def replicate_to_replicas(self, file_name, data):
        """Send the append operation to all available replicas and wait for acknowledgments."""
        message = json.dumps({"action": "replicate", "file": file_name, "data": data})
        for peer in self.peers:
            try:
                self.sock.sendto(message.encode(), (peer[0], peer[1]))
                # Wait for acknowledgment for strong consistency
                ack, _ = self.sock.recvfrom(1024)
                if ack.decode() != "ack":
                    print(f"[DEBUG] No ack received from {peer}, retrying...")
            except socket.timeout:
                print(f"[DEBUG] Replica {peer} is unreachable.")

    def monitor_lock_expiration(self):
        while True:
            with self.lock:
                if self.current_lock_holder and self.lock_expiration_time and time.time() > self.lock_expiration_time:
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.save_state()
            time.sleep(1)
    
    def handle_recovery_sync(self, client_id):
        """Handle synchronization for a recovering replica."""
        for i in range(NUM_FILES):
            file_name = f"file_{i}"
            file_path = os.path.join(FILES_DIR, file_name)
            with open(file_path, 'r') as f:
                data = f.read()
            recovery_message = json.dumps({"action": "recover", "file": file_name, "data": data})
            self.sock.sendto(recovery_message.encode(), (self.server_address[0], self.server_address[1] + 1))

    def handle_replicate(self, file_name, data):
        """Handle replication requests from the leader to update local files."""
        file_path = os.path.join(FILES_DIR, file_name)
        with open(file_path, 'a') as f:
            f.write(data + "\n")
        print(f"[DEBUG] Replica updated {file_name} with '{data}'")
        self.sock.sendto("ack".encode(), self.leader_address)  # Send acknowledgment

    def handle_messages(self):
        while True:
            try:
                data, addr = self.raft_sock.recvfrom(1024)
                message = json.loads(data.decode())
                if message["action"] == "replicate":
                    self.handle_replicate(message["file"], message["data"])
                elif message["action"] == "recover":
                    self.handle_replicate(message["file"], message["data"])
            except socket.timeout:
                continue

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
            parts = message.split(":")
            if len(parts) == 4:
                client_id = parts[1]
                file_name = parts[2]
                file_data = parts[3]
                response = self.append_to_file(client_id, file_name, file_data)
                self.sock.sendto(response.encode(), client_address)
                print(f"[DEBUG] Sent append_file response: {response}")
            else:
                print(f"[ERROR] Malformed append_file message: {message}")

        elif message == "identify_leader":
            response = "I am the leader" if self.role == 'leader' else f"Redirect to leader at {self.leader_address}"
            self.sock.sendto(response.encode(), client_address)

    def heartbeat_check(self):
        while True:
            if self.role == 'follower' and (time.time() - self.last_heartbeat) > self.election_timeout:
                print(f"[DEBUG] Server {self.server_id} detected leader failure, starting election")
                self.start_election()
            time.sleep(0.1)

    def start_election(self):
        if self.server_id != 1:  # Only non-primary servers initiate elections
            self.role = 'candidate'
            self.term += 1
            votes = 1
            for peer in self.peers:
                response = self.request_vote(peer, self.term, self.server_id)
                if response == "vote_granted":
                    votes += 1
            if votes > len(self.peers) // 2:
                self.role = 'leader'
                self.leader_address = self.server_address
                print(f"[DEBUG] Server {self.server_id} became the leader for term {self.term}")
                self.send_heartbeats()
            else:
                self.role = 'follower'

    def request_vote(self, peer, term, candidate_id):
        message = f"request_vote:{term}:{candidate_id}"
        self.sock.sendto(message.encode(), peer)

    def send_heartbeats(self):
        while self.role == 'leader':
            message = f"heartbeat:{self.term}:{self.server_id}"
            for peer in self.peers:
                self.sock.sendto(message.encode(), peer)
            print(f"[DEBUG] Leader {self.server_id} sent heartbeat")  # Add this line for confirmation
            time.sleep(0.5)

    def save_state(self):
        """Save the current lock state to a JSON file for persistence."""
        state = {
            "current_lock_holder": self.current_lock_holder,
            "lock_expiration_time": self.lock_expiration_time
        }
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(state, f)
            print("[DEBUG] Server state saved successfully.")
        except IOError as e:
            print(f"[ERROR] Failed to save state: {e}")

    def load_state(self):
        """Load the lock state from a JSON file, if it exists."""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    state = json.load(f)
                self.current_lock_holder = state.get("current_lock_holder")
                self.lock_expiration_time = state.get("lock_expiration_time")
                print("[DEBUG] Server state loaded successfully.")
            except IOError as e:
                print(f"[ERROR] Failed to load state: {e}")
            except json.JSONDecodeError as e:
                print(f"[ERROR] Failed to decode state file: {e}")
        else:
            print("[DEBUG] No state file found. Starting with default state.")

if __name__ == "__main__":
    server = LockManagerServer()
    server.start()