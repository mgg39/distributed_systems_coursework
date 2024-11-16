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
    def __init__(self, host='localhost', port=8080, server_id=1, peers =[]): #peers=[("localhost", 8083), ("localhost", 8085)]):
        self.queue_clients = []  # Ensure queue_clients is always initialized
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
        self.lock = threading.Lock() #reentrant locking
        self.current_lock_holder = None
        self.lock_expiration_time = None
        if hasattr(self, "queue_clients"):
            print(f"[DEBUG] Lock released. Queue before processing: {self.queue_clients}")
        else:
            print("[DEBUG] Lock released but queue_clients is not initialized.")
        self.process_queue()
        print(f"[DEBUG] Queue after processing: {self.queue_clients}")
        self.request_history = defaultdict(dict)

        self.queue_clients = []

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
                    #TODO: respond - aknowledge updates
                    if self.sync_lock_state(lock_data) == True:
                        self.sock.sendto(message.encode(), self.leader_address)

                elif parts[0] == "file_update":
                    file_name, file_data = parts[1], parts[2]
                    success = self.sync_file(file_name, file_data)
                    if success:
                        response = f"file_update_ack:{file_name}"
                        self.raft_sock.sendto(response.encode(), addr)

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
        print(f"[DEBUG] Follower syncing file {file_name} with data: {file_data}")
        try:
            file_path = os.path.join(FILES_DIR, file_name)
            with open(file_path, 'a') as f:
                f.write(file_data + "\n")
                f.flush()
                os.fsync(f.fileno())
            print(f"[DEBUG] Successfully synced file {file_name}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to sync file {file_name}: {e}")
            return False

    #----------Replica logic---------------

    def queue(self, client_id):
        if client_id not in self.queue_clients:
            self.queue_clients.append(client_id)
        self.process_queue()
    
    def notify_client_lock_granted(self, client_id):
        client_address = self.get_client_address(client_id)
        if client_address:
            message = f"grant lock:{LOCK_LEASE_DURATION}"
            self.sock.sendto(message.encode(), client_address)
            print(f"[DEBUG] Notified {client_id} about lock grant.")

    def process_queue(self):
        print(f"[DEBUG] Starting process_queue. Current queue: {self.queue_clients}, Current holder: {self.current_lock_holder}")
        if self.current_lock_holder:
            print(f"[DEBUG] Skipping process_queue - Lock still held by {self.current_lock_holder}.")
        elif not self.queue_clients:
            print(f"[DEBUG] Skipping process_queue - Queue is empty.")
        else:
            print(f"[DEBUG] Queue is non-empty and lock is available. Processing next client.")
        if self.current_lock_holder:
            print(f"[DEBUG] Lock is currently held by {self.current_lock_holder}. Skipping queue processing.")
        with self.lock:
            if not self.current_lock_holder and self.queue_clients:
                next_client = self.queue_clients.pop(0)
                self.current_lock_holder = next_client
                self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                print(f"[DEBUG] Lock assigned to {self.current_lock_holder}. Updated queue: {self.queue_clients}")
                self.save_state()
                self.notify_followers_lock_state()
                self.notify_client_lock_granted(next_client)
                print(f"[DEBUG] Lock granted to {next_client}. Notification sent.")
                print(f"[DEBUG] Lock granted to {next_client}. Notifying the client.")
                self.notify_client_lock_granted(next_client)
            else:
                if self.current_lock_holder:
                    print(f"[DEBUG] Queue processing skipped. Lock already held by {self.current_lock_holder}.")
                elif not self.queue_clients:
                    print(f"[DEBUG] Queue is empty. No further processing required.")
        print(f"[DEBUG] Processing queue. Current state: {self.queue_clients}")
        with self.lock:
            if not self.current_lock_holder and self.queue_clients:
                next_client = self.queue_clients.pop(0)
                self.current_lock_holder = next_client
                self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                print(f"[DEBUG] Processed queue, current lock holder: {self.current_lock_holder}")
                self.save_state()
                self.notify_followers_lock_state()
                self.notify_client_lock_granted(next_client)
                print(f"[DEBUG] Lock granted to {next_client}. Notification sent.")
                print(f"[DEBUG] Lock granted to {next_client}. Notifying the client.")
                self.notify_client_lock_granted(next_client)

    def acquire_lock(self, client_id):
        print(f"[DEBUG] Client {client_id} entered acquire lock")
        
        if self.role == 'leader':
            print(f"[DEBUG] Client {client_id} entered acquire lock at leader")
            
            with self.lock:
                print(f"[DEBUG] Client {client_id} entered acquire lock past self.lock")
                current_time = time.time()

                # Grant the lock if no current holder or lock is expired
                if not self.current_lock_holder or (self.lock_expiration_time and current_time > self.lock_expiration_time):
                    self.current_lock_holder = client_id
                    self.lock_expiration_time = current_time + LOCK_LEASE_DURATION
                    self.notify_followers_lock_state()
                    self.save_state()
                    print(f"[DEBUG] Lock granted to {client_id}")
                    return f"grant lock:{LOCK_LEASE_DURATION}"

                # Check if client is already in queue
                if client_id in self.queue_clients:
                    position = self.queue_clients.index(client_id) + 1
                    print(f"[DEBUG] Client {client_id} already in queue at position {position}")
                    return f"queued:position_{position}"

                # Add client to queue
                self.queue_clients.append(client_id)
                position = len(self.queue_clients)
                print(f"[DEBUG] Client {client_id} added to queue at position {position}")
                return f"queued:position_{position}"
        else:
            # If not the leader, redirect client to leader
            if self.leader_address:
                return f"Redirect to leader at {self.leader_address}"
            else:
                return "Leader unknown"
                
    def release_lock(self, client_id):
        print(f"[DEBUG] release_lock called by {client_id}. Current holder: {self.current_lock_holder}")
        try:
            print(f"[DEBUG] Release lock called by {client_id}. Current holder: {self.current_lock_holder}")
            with self.lock:
                if self.current_lock_holder == client_id:
                    print(f"[DEBUG] Releasing lock for {client_id}")
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    
                    # Remove client from queue if present
                    if client_id in self.queue_clients:
                        self.queue_clients.remove(client_id)
                    
                    # Notify followers about lock state change
                    if self.role == 'leader':
                        self.notify_followers_lock_state()
                    
                    self.save_state()
                    self.process_queue()  # Process next client in queue
                    print(f"[DEBUG] Lock successfully released by {client_id}")
                    return "unlock success"
                else:
                    print(f"[DEBUG] Release failed - {client_id} does not hold the lock. Current holder: {self.current_lock_holder}")
                    return "You do not hold the lock"
        except Exception as e:
            print(f"[ERROR] Exception in release_lock: {e}")
            return "release failed"


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
        # Optionally wait for acknowledgment here

    def notify_followers_lock_state(self):
        lock_data = json.dumps({
            "current_lock_holder": self.current_lock_holder,
            "lock_expiration_time": self.lock_expiration_time
        })
        for peer in self.peers:
            message = f"lock_state:{lock_data}"
            self.sock.sendto(message.encode(), peer)

    #----------Replica logic---------------

    def get_client_address(self, client_id):
        """
        Retrieve the client address from request history.
        """
        return self.request_history.get(client_id, {}).get("address")


    def notify_client_lock_expired(self, client_id):
        client_address = self.get_client_address(client_id)
        if client_address:
            self.sock.sendto("lock expired".encode(), client_address)
            print(f"[DEBUG] Notified {client_id} about lock expiration.")


    def monitor_lock_expiration(self):
        print("[DEBUG] Starting monitor_lock_expiration thread.")
        while True:
            with self.lock:
                if self.current_lock_holder and self.lock_expiration_time and time.time() > self.lock_expiration_time:
                    print(f"[DEBUG] Lock expired for client {self.current_lock_holder}. Releasing lock.")
                    expired_client = self.current_lock_holder
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.save_state()
                    self.process_queue()  # Process the next client in the queue
                    self.notify_client_lock_expired(expired_client)
                    print(f"[DEBUG] Lock expired for {expired_client}. Triggering process_queue.")
                    self.process_queue()
            time.sleep(1)

    def notify_client_lock_released(self, client_id):
        client_address = self.get_client_address(client_id)
        if client_address:
            self.sock.sendto("lock expired".encode(), client_address)

    def handle_request(self, data, client_address):
        message = data.decode()
        print(f"[DEBUG] Received request: {message} from {client_address}")
        
        # Store client address in request history for notifications
        parts = message.split(":")
        if len(parts) >= 2:
            client_id = parts[1]
            if client_id not in self.request_history:
                self.request_history[client_id] = {}
            self.request_history[client_id]["address"] = client_address

        if message.startswith("acquire_lock"):
            client_id = message.split(":")[1]
            response = self.acquire_lock(client_id)
        elif message.startswith("release_lock"):
            client_id = message.split(":")[1]
            response = self.release_lock(client_id)
        elif message.startswith("append_file"):
            client_id, request_id, file_name, file_data = message.split(":")[1:5]
            response = self.append_to_file(client_id, file_name, file_data)
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
        elif message.startswith("release_lock"):
            client_id = message.split(":")[1]
            response = self.release_lock(client_id)
            self.sock.sendto(response.encode(), client_address)
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
                try:
                    print(f"[DEBUG] Sending heartbeat to {peer}")
                    self.sock.sendto(message.encode(), peer)
                except Exception as e:
                    print(f"[ERROR] Failed to send heartbeat to {peer}: {e}")
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

    def shutdown(self):
        self.running = False
        self.socket.close()
        print("[DEBUG] Server shutdown.")

if __name__ == "__main__":
    server = LockManagerServer(("localhost", 8080))
    try:
        server.start()
        while True:
            pass  # Keep server running
    except KeyboardInterrupt:
        server.shutdown()