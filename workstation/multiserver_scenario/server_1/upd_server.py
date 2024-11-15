import socket
import os
import threading
import time
import random
import json
import urllib.parse
from queue import Queue
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

        # Initialization lock queue
        self.request_queue = Queue()  # Queue for managing lock requests
        self.processing_lock = threading.Lock()  # Lock for queue processing
        self.current_lock_holder = None
        self.lock_expiration_time = None

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
    
    def debug_queue_state(self):
        print(f"[DEBUG] Current lock holder: {self.current_lock_holder}")
        print(f"[DEBUG] Lock expiration time: {self.lock_expiration_time}")
        print(f"[DEBUG] Lock queue size: {self.request_queue.qsize()}")

    
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
                    if self.sync_file(lock_data) == True:
                        self.sock.sendto(message.encode(), self.leader_address)
                
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
        print(f"[DEBUG] Follower syncing file {file_name} with data: {file_data}")
        file_path = os.path.join(FILES_DIR, file_name)
        with open(file_path, 'a') as f:
            f.write(file_data + "\n")
            f.flush()
            os.fsync(f.fileno())
        print(f"[DEBUG] Synchronized file {file_name} with data: {file_data} on follower")
        return True
    #----------Replica logic---------------

    def acquire_lock_request(self, client_id):
        if self.role == 'leader':
            with self.processing_lock:
                if not self.current_lock_holder:
                    # Grant the lock immediately if available
                    self.current_lock_holder = client_id
                    self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                    print(f"[DEBUG] Lock granted to {client_id} with lease duration of {LOCK_LEASE_DURATION} seconds.")
                    return f"grant lock:{LOCK_LEASE_DURATION}"
                else:
                    # Add client to queue if lock is held
                    self.request_queue.put(client_id)
                    print(f"[DEBUG] {client_id} added to lock request queue.")
                    return f"Request for lock by {client_id} added to the queue."
            debug_queue_state(self)
        else:
            return f"Redirect to leader at {self.leader_address}" 
        
    def process_lock_queue(self):
        with self.processing_lock:
            if not self.current_lock_holder and not self.request_queue.empty():
                client_id = self.request_queue.get()  # Get the next client in line
                self.current_lock_holder = client_id
                self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                if hasattr(self, "renewal_count"):
                    self.renewal_count[client_id] = 0  # Reset renewal count for fairness
                print(f"[DEBUG] Lock granted to {client_id} from queue with lease duration of {LOCK_LEASE_DURATION} seconds.")
                self.notify_followers_lock_state()
            elif self.current_lock_holder:
                print(f"[DEBUG] Lock is still held by {self.current_lock_holder}.")
            else:
                print(f"[DEBUG] Lock queue is empty.")
        self.debug_queue_state()


    def release_lock(self, client_id):
        with self.lock:
            if self.current_lock_holder == client_id:
                self.current_lock_holder = None
                self.lock_expiration_time = None
                self.save_state()
                debug_queue_state(self)
                return "unlock success"
            else:
                return "You do not hold the lock"

    def renew_lease(self, client_id):
        with self.lock:
            if self.current_lock_holder == client_id:
                # Limit renewals
                if not hasattr(self, "renewal_count"):
                    self.renewal_count = defaultdict(int)
                self.renewal_count[client_id] += 1
                
                # Allow only 3 renewals per client for fairness
                if self.renewal_count[client_id] > 3:
                    print(f"[DEBUG] Client {client_id} exceeded lease renewal limit. Releasing lock.")
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.renewal_count[client_id] = 0
                    threading.Thread(target=self.process_lock_queue, daemon=True).start()
                    return "lease limit exceeded"
                
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
        success = True  # Assume success initially
        for peer in self.peers:
            message = f"file_update:{file_name}:{file_data}"
            try:
                self.sock.sendto(message.encode(), peer)
                # Wait for acknowledgment
                self.sock.settimeout(2)  # Timeout for receiving acknowledgment
                response, _ = self.sock.recvfrom(1024)
                if response.decode() != "ack":
                    print(f"[DEBUG] Failed acknowledgment from peer {peer}")
                    success = False
            except socket.timeout:
                print(f"[DEBUG] Timeout waiting for acknowledgment from peer {peer}")
                success = False
            except Exception as e:
                print(f"[DEBUG] Error sending update to peer {peer}: {e}")
                success = False
        return success

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
            with self.processing_lock:
                if self.current_lock_holder and self.lock_expiration_time and time.time() > self.lock_expiration_time:
                    print(f"[DEBUG] Lock expired for client {self.current_lock_holder}")
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    # Randomized delay before processing the queue
                    time.sleep(random.uniform(0.1, 0.5))
                    threading.Thread(target=self.process_lock_queue, daemon=True).start()
            time.sleep(1)

    def process_client_lease_check(self, client_id):
        if self.current_lock_holder == client_id:
            self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
            return "lease renewed"
        elif client_id in list(self.request_queue.queue):
            return "Lock is still pending"
        else:
            return "No lock request found"

    
    def handle_request(self, data, client_address):
        message = data.decode()
        print(f"[DEBUG] Received request: {message} from {client_address}")

        if message.startswith("acquire_lock_request"):
            client_id = message.split(":")[1]
            response = self.acquire_lock_request(client_id)
        elif message.startswith("release_lock"):
            client_id = message.split(":")[1]
            response = self.release_lock(client_id)
        elif message.startswith("append_file"):
            # Decode the message to handle URL-encoded data
            decoded_message = urllib.parse.unquote(message)
            try:
                client_id, request_id, file_name, file_data = decoded_message.split(":")[1:5]
                response = self.append_to_file(client_id, file_name, file_data)
            except ValueError as e:
                print(f"[ERROR] Failed to parse append_file message: {decoded_message}, error: {e}")
                response = "Invalid append_file command"
        elif message.startswith("identify_leader"):
            if self.role == 'leader':
                response = "I am the leader"
            else:
                if self.leader_address:
                    response = f"Redirect to leader:{self.leader_address[0]}:{self.leader_address[1]}"
                else:
                    response = "Leader unknown"
        elif message.startswith("client_lease_check"):
            client_id = message.split(":")[1]
            response = self.process_client_lease_check(client_id)
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
    
    def process_leader_heartbeat(self, leader_id, term):
        # Logic for handling leader heartbeat and updating leader information
        print(f"[DEBUG] Received heartbeat from leader {leader_id} with term {term}")
        if term > self.term:
            self.term = term
            self.role = 'follower'
            self.current_leader = leader_id
            self.last_heartbeat = time.time()
            print(f"[DEBUG] Updated leader to {leader_id} with term {term}")
        elif term == self.term:
            self.last_heartbeat = time.time()  # Renew last heartbeat time
        # Ignore heartbeats with a lower term

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