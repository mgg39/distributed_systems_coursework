import socket
import os
import threading
import time
from collections import defaultdict
import urllib.parse
import json
import random

FILES_DIR = "server_files"
NUM_FILES = 100
LOCK_LEASE_DURATION = 20  # Lease duration in seconds
STATE_FILE = os.path.join(os.path.dirname(__file__), "server_state_3.json")

# Ensure the files directory exists and create 100 files
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)
for i in range(NUM_FILES):
    open(os.path.join(FILES_DIR, f"file_{i}"), 'a').close()

class LockManagerServer:
    def __init__(self, host='localhost', port=8084, server_id=3, peers=[("localhost", 8081), ("localhost", 8083)]):
        self.server_address = (host, port)
        self.server_id = server_id
        self.peers = peers
        self.role = 'leader' if self.server_id == 1 else 'follower'  # Server 1 is initially the leader
        self.leader_address = self.server_address if self.role == 'leader' else None
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(1, 2)

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

        self.queue_clients = []
        self.lock_acquired = False

        self.modif_request = {}  # Dictionary to store client modifications
        self.clients_active = []  # List of active clients


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
                    leader_id = int(parts[1])
                    self.process_heartbeat(leader_id)

                elif parts[0] == "lock_state":
                    lock_data = parts[1]
                    self.sync_lock_state(lock_data)
                    if self.sync_lock_state(lock_data) == True:
                        self.sock.sendto(message.encode(), self.leader_address)

                elif parts[0] == "file_update":
                    file_name, file_data = parts[1], parts[2]
                    self.sync_file(file_name, file_data)
                    #if self.sync_file(file_name, file_data) == False:
                        #self.sock.sendto(message.encode(), self.leader_address)
                
                elif message == "identify_leader":
                    if self.role == 'leader':
                        response = "I am the leader"
                    else:
                        if self.leader_address:
                            response = f"Redirect to leader:{self.leader_address[0]}:{self.leader_address[1]}"
                        else:
                            response = "Leader unknown"
                    self.sock.sendto(response.encode(), client_address)
                    
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[ERROR] handle_messages error: {e}")
    
    def process_heartbeat(self, leader_id):
        if leader_id < self.server_id:
            self.role = 'follower'
            self.leader_address = self.peers[leader_id - 1]
            self.last_heartbeat = time.time()
            print(f"[DEBUG] Server {self.server_id} following leader {leader_id}.")

    #----------Replica logic---------------
    def sync_lock_state(self, lock_data):
        # Synchronize lock state from leader
        with self.lock:
            lock_state = json.loads(lock_data)
            self.current_lock_holder = lock_state.get("current_lock_holder")
            self.lock_expiration_time = lock_state.get("lock_expiration_time")
            print(f"[DEBUG] Synchronized lock state from leader")
            return True

    def sync_file(self, file_name, file_data): 
        # Synchronize file append from leader
        try:
            print(f"[DEBUG] Follower syncing file {file_name} with data: {file_data}")
            file_path = os.path.join(FILES_DIR, file_name)
            with open(file_path, 'a') as f:
                f.write(file_data + "\n")
                f.flush()
                os.fsync(f.fileno())
            print(f"[DEBUG] Synchronized file {file_name} with data: {file_data} on follower")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to sync file {file_name}: {str(e)}")
            return False #Not working as expected
    #----------Replica logic---------------

    def queue(self, client_id):
        if client_id not in self.queue_clients:
            self.queue_clients.append(client_id)
        self.process_queue()

    def process_queue(self):
        with self.lock:
            if not self.current_lock_holder and self.queue_clients:
                next_client = self.queue_clients.pop(0)  # Get the next client in line
                self.current_lock_holder = next_client
                self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                self.notify_followers_lock_state()
                self.save_state()
                print(f"[DEBUG] Lock granted to {next_client}.")

    def acquire_lock(self, client_id):
        if self.role == 'leader':
            with self.lock:
                current_time = time.time()

                # Check if the lock can be granted
                if not self.current_lock_holder or (self.lock_expiration_time and current_time > self.lock_expiration_time):
                    # Grant the lock to the requesting client
                    self.current_lock_holder = client_id
                    self.lock_expiration_time = current_time + LOCK_LEASE_DURATION
                    self.lock_acquired = True
                    self.notify_followers_lock_state()
                    self.save_state()
                    return f"grant lock:{LOCK_LEASE_DURATION}"
                else:
                    # Add client to queue
                    if client_id not in self.queue_clients:
                        self.queue_clients.append(client_id)
                    position = self.queue_clients.index(client_id) + 1
                    return f"queued:position_{position}"
        else:
            return f"Redirect to leader at {self.leader_address}"

    def release_lock(self, client_id):
        if self.role == 'leader':
            with self.lock:
                if self.current_lock_holder == client_id:
                    # Release the lock
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.lock_acquired = False
                    
                    # Notify followers of the updated lock state
                    self.notify_followers_lock_state()
                    
                    # Persist state
                    self.save_state()
                    
                    print(f"[DEBUG] Lock released successfully by {client_id}.")
                    return "unlock success"
                else:
                    print(f"[DEBUG] Release lock failed: {client_id} does not hold the lock.")
                    return "You do not hold the lock"
        else:
            # Redirect to the leader if this server is not the leader
            return f"Redirect to leader at {self.leader_address}"

    
    def get_queue_status(self, client_id):
        with self.lock:
            if self.current_lock_holder == client_id:
                return "You hold the lock"
            elif client_id in self.queue_clients:
                position = self.queue_clients.index(client_id) + 1
                return f"queued:position_{position}"
            else:
                return "You are not in the queue"

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
                self.notify_followers_file_update(file_name, data)
                return "append success"
            else:
                return "File not found"
        else:
            return "You do not hold the lock"
        
    #----------Replica logic---------------

    def notify_followers_file_update(self, file_name, file_data):
        print(f"[DEBUG] Leader sending file update for {file_name} with data: {file_data}")
        for peer in self.peers:
            message = f"file_update:{file_name}:{file_data}"
            self.sock.sendto(message.encode(), peer)

    def notify_followers_lock_state(self):
        lock_data = json.dumps({
            "current_lock_holder": self.current_lock_holder,
            "lock_expiration_time": self.lock_expiration_time
        })
        for peer in self.peers:
            message = f"lock_state:{lock_data}"
            self.sock.sendto(message.encode(), peer)

    #----------Replica logic---------------

    def monitor_lock_expiration(self):
        while True:
            with self.lock:
                if self.current_lock_holder and self.lock_expiration_time and time.time() > self.lock_expiration_time:
                    print(f"[DEBUG] Lock expired for client {self.current_lock_holder}. Releasing lock.")
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.save_state()
                    self.process_queue()  # Process the next client in the queue
            time.sleep(1)

    
    def handle_request(self, data, client_address):
        message = data.decode()
        print(f"[DEBUG] Received request: {message} from {client_address}")

        if message.startswith("acquire_lock"):
            client_id = message.split(":")[1]
            response = self.acquire_lock(client_id)
        elif message.startswith("release_lock"):
            client_id = message.split(":")[1]
            response = self.release_lock(client_id)
        elif message.startswith("append_file"):
            client_id, request_id, file_name, file_data = message.split(":")[1:5]
            if client_id in self.clients_active:
                # Check if client_id exists in modif_request and if request_id is in its values
                if client_id in self.modif_request and request_id in self.modif_request[client_id]:
                    response = "Repeated command"
                else:
                    response = self.append_to_file(client_id, file_name, file_data)
                    # If client_id doesn't exist in modif_request, create a new set
                    if client_id not in self.modif_request:
                        self.modif_request[client_id] = set()
                    # Add request_id to the set for this client
                    self.modif_request[client_id].add(request_id)
            else:
                response = self.append_to_file(client_id, file_name, file_data)
                # Initialize a new set for this client and add request_id
                self.modif_request[client_id] = {request_id}
        elif message.startswith("identify_leader"):
            if self.role == 'leader':
                response = "I am the leader"
            else:
                if self.leader_address:
                    response = f"Redirect to leader:{self.leader_address[0]}:{self.leader_address[1]}"
                else:
                    response = "Leader unknown"
        elif message == "ping":
            response = "pong"
        else:
            response = "Unknown command"

        print(f"[DEBUG] Sending response: {response} to {client_address}")
        self.sock.sendto(response.encode(), client_address)
    
    def heartbeat_check(self):
        while True:
            if self.role == 'follower' and (time.time() - self.last_heartbeat) > self.election_timeout:
                print(f"[DEBUG] Server {self.server_id} detected leader failure, starting election")
                self.start_election()
            time.sleep(0.1)
    
    def start_election(self):
        if self.role == 'leader':
            return  # Do nothing if already a leader

        # Initialize the flag
        lowest_active_peer_found = False

        # Check each lower-ID server to see if it’s alive
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


    def send_heartbeats(self):
        while self.role == 'leader':
            message = f"heartbeat:{self.server_id}"
            for peer in self.peers:
                print(f"[DEBUG] Sending heartbeat to {peer}")
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