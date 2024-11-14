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
        self.role = 'leader' if self.server_id == 1 else 'follower'
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
        self.raft_sock.settimeout(1)

        # Lock management
        self.lock = threading.Lock()
        self.current_lock_holder = None
        self.lock_expiration_time = None

        # Load state for fault tolerance
        self.load_state()

        # Start threads
        if self.role == 'leader':
            threading.Thread(target=self.send_heartbeats, daemon=True).start()
        threading.Thread(target=self.monitor_lock_expiration, daemon=True).start()
        threading.Thread(target=self.heartbeat_check, daemon=True).start()
        threading.Thread(target=self.handle_messages, daemon=True).start()

    def start(self):
        print(f"Server {self.server_id} listening on {self.server_address}")
        while True:
            data, client_address = self.sock.recvfrom(1024)
            threading.Thread(target=self.handle_request, args=(data, client_address)).start()

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
                client_id, file_name, file_data = parts[1:4]
                response = self.append_to_file(client_id, file_name, file_data)
                self.sock.sendto(response.encode(), client_address)

    def acquire_lock(self, client_id):
        if self.role == 'leader':
            with self.lock:
                if not self.current_lock_holder or time.time() > self.lock_expiration_time:
                    self.current_lock_holder = client_id
                    self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                    self.save_state()
                    return f"grant lock:{LOCK_LEASE_DURATION}"
                else:
                    return "Lock is currently held"
        return f"Redirect to leader at {self.leader_address}"

    def release_lock(self, client_id):
        with self.lock:
            if self.current_lock_holder == client_id:
                self.current_lock_holder, self.lock_expiration_time = None, None
                self.save_state()
                return "unlock success"
            return "You do not hold the lock"

    def append_to_file(self, client_id, file_name, data):
        if self.current_lock_holder == client_id:
            file_path = os.path.join(FILES_DIR, file_name)
            with open(file_path, 'a') as f:
                f.write(data + "\n")
            print(f"[DEBUG] {client_id} appended to {file_name}: '{data}'")
            self.replicate_to_replicas(file_name, data)
            return "append success"
        return "You do not hold the lock"
    
    def replicate_to_replicas(self, file_name, data):
        message = json.dumps({"action": "replicate", "file": file_name, "data": data})
        for peer in self.peers:
            self.sock.sendto(message.encode(), peer)

    def handle_messages(self):
        while True:
            try:
                data, addr = self.raft_sock.recvfrom(1024)
                if not data:
                    continue
                message = data.decode()
                parts = message.split(":")

                if parts[0] == "heartbeat":
                    self.last_heartbeat = time.time()
                elif parts[0] == "request_vote":
                    self.raft_sock.sendto("vote_granted".encode(), addr)

            except socket.timeout:
                continue
            except Exception as e:
                print(f"[ERROR] handle_messages error: {e}")

    def heartbeat_check(self):
        while True:
            if self.role == 'follower' and (time.time() - self.last_heartbeat) > self.election_timeout:
                self.start_election()
            time.sleep(0.1)

    def start_election(self):
        print(f"[DEBUG] Server {self.server_id} starting election")
        self.role = 'candidate'
        self.term += 1
        self.send_heartbeats()  # Notify peers

    def send_heartbeats(self):
        while self.role == 'leader':
            message = f"heartbeat:{self.term}:{self.server_id}"
            for peer in self.peers:
                self.sock.sendto(message.encode(), peer)
            time.sleep(0.5)

    def monitor_lock_expiration(self):
        while True:
            with self.lock:
                if self.current_lock_holder and time.time() > self.lock_expiration_time:
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.save_state()
            time.sleep(1)

    def save_state(self):
        state = {
            "current_lock_holder": self.current_lock_holder,
            "lock_expiration_time": self.lock_expiration_time
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
        print("[DEBUG] Server state saved successfully.")

    def load_state(self):
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            self.current_lock_holder = state.get("current_lock_holder")
            self.lock_expiration_time = state.get("lock_expiration_time")
            print("[DEBUG] Server state loaded successfully.")

if __name__ == "__main__":
    server = LockManagerServer()
    server.start()
