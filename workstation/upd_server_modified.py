import os
import socket
import threading
import time
import random
from collections import defaultdict
import json
import urllib

# Global templates for unique files and directories per server
STATE_FILE_TEMPLATE = "server_state_{}.json"  # individual state files
FILES_DIR_TEMPLATE = "server_files_{}"  # individual directories
WORKING_DIR_TEMPLATE = "server_working_dir_{}"  # individual working directories
NUM_FILES = 100
LOCK_LEASE_DURATION = 20  # Lease duration in seconds

class LockManagerServer:
    def __init__(self, host='localhost', port=8080, server_id=0, peers=[]):
        print(f"[DEBUG] Initializing server {server_id}")

        self.server_id = server_id  # Unique ID for the server
        self.server_address = (host, port)
        self.peers = peers  # List of (host, port) tuples for peer servers
        self.term = 0  # Current term for Raft
        self.voted_for = None  # Candidate ID that received vote in current term
        self.role = 'follower'  # Start as a follower
        self.leader_address = None  # Leader’s address tracking
        self.election_timeout = random.uniform(15, 30)  # Randomized timeout for leader election
        self.last_heartbeat = time.time()  # Track last heartbeat for election timeout
        self.votes = 0  # Track votes received during an election

        # Lock management attributes
        self.lock = threading.Lock()
        self.current_lock_holder = None
        self.lock_expiration_time = None  # Stores when the lock should expire
        self.request_history = defaultdict(dict)  # Stores each client's request history

        # Define unique working and file directories for each server
        self.WORKING_DIR = WORKING_DIR_TEMPLATE.format(self.server_id)
        self.FILES_DIR = FILES_DIR_TEMPLATE.format(self.server_id)
        
        # Ensure unique working directory exists
        if not os.path.exists(self.WORKING_DIR):
            os.makedirs(self.WORKING_DIR)
            print(f"[DEBUG] Created working directory: {self.WORKING_DIR}")

        # Set PYTHONPYCACHEPREFIX to isolate __pycache__ for each server
        #os.environ['PYTHONPYCACHEPREFIX'] = os.path.join(self.WORKING_DIR, "__pycache__")
        
        # Ensure the server-specific files directory exists and initialize files
        if not os.path.exists(self.FILES_DIR):
            os.makedirs(self.FILES_DIR)
            print(f"[DEBUG] Created files directory: {self.FILES_DIR}")
        
        # Initialize the 100 files in the server-specific directory
        for i in range(NUM_FILES):
            file_path = os.path.join(self.FILES_DIR, f"file_{i}_{self.server_id}")
            if not os.path.exists(file_path):  # Create file only if it does not already exist
                open(file_path, 'a').close()
                print(f"[DEBUG] Created file: {file_path}")

        # Define unique state file for each server
        self.STATE_FILE = STATE_FILE_TEMPLATE.format(self.server_id)  # Unique log file for each server
        self.load_state()

        # Socket for client requests
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.server_address)
        
        # Socket for Raft protocol messages
        self.raft_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.raft_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.raft_sock.bind((host, port + 1))  # Bind to a different port

        # Start background threads for Raft and lock expiration
        threading.Thread(target=self.monitor_lock_expiration, daemon=True).start()
        threading.Thread(target=self.heartbeat_check, daemon=True).start()
        threading.Thread(target=self.handle_messages, daemon=True).start()

    def load_state(self):
        if os.path.exists(self.STATE_FILE):
            with open(self.STATE_FILE, "r") as f:
                state = json.load(f)
            self.current_lock_holder = state.get("current_lock_holder")
            self.lock_expiration_time = state.get("lock_expiration_time")
            self.request_history = state.get("request_history", defaultdict(dict))
            print("[DEBUG] Server state loaded.")
        else:
            print("[DEBUG] No existing state file to load.")

    def start(self):
        print(f"Server {self.server_id} listening on {self.server_address}")
        while True:
            data, client_address = self.sock.recvfrom(1024)
            threading.Thread(target=self.handle_request, args=(data, client_address)).start()

    def acquire_lock(self, client_id):
        print(f"[DEBUG] acquire_lock invoked by {client_id}, role: {self.role}")
        if self.role == 'leader':
            with self.lock:
                current_time = time.time()
                if not self.current_lock_holder or (self.lock_expiration_time and current_time > self.lock_expiration_time):
                    self.current_lock_holder = client_id
                    self.lock_expiration_time = current_time + LOCK_LEASE_DURATION
                    self.save_state()
                    print(f"[DEBUG] Lock granted to {client_id} with lease duration {LOCK_LEASE_DURATION}")
                    return f"grant lock:{LOCK_LEASE_DURATION}"
                else:
                    print(f"[DEBUG] Lock currently held by {self.current_lock_holder}, cannot grant to {client_id}")
                    return "Lock is currently held"
        else:
            print(f"[DEBUG] Redirecting to leader as role is not leader")
            return "Redirect to leader"

    def release_lock(self, client_id):
        # Release lock if held by the requesting client
        with self.lock:
            if self.current_lock_holder == client_id:
                self.current_lock_holder = None
                self.lock_expiration_time = None
                self.save_state()  # Save state after releasing the lock
                print(f"{client_id} released the lock.")
                return "unlock success"
            else:
                return "You do not hold the lock"

    def renew_lease(self, client_id):
        # Renew lease if client holding lock sends a heartbeat
        with self.lock:
            if self.current_lock_holder == client_id:
                self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                print(f"Lease renewed by {client_id}. New expiration time: {self.lock_expiration_time}")
                return "lease renewed"
            else:
                return "You do not hold the lock"
    
    def replicate_append(self, file_name, data):
        message = f"append_file:{self.server_id}:{file_name}:{data}"
        for peer in self.peers:
            try:
                self.sock.sendto(message.encode(), peer)  # Send append request to each peer
            except Exception as e:
                print(f"Error replicating to peer {peer}: {e}")

    def append_to_file(self, client_id, file_name, data):
        # Append data to file if lock is held by the requesting client
        with self.lock:
            if self.current_lock_holder == client_id:
                file_path = os.path.join(self.FILES_DIR, file_name)

                # Check if file exists before writing
                if os.path.exists(file_path):
                    with open(file_path, 'a') as f:
                        f.write(data + "\n")
                        f.flush()  # Flush internal buffer
                        os.fsync(f.fileno())  # Force write to disk

                    # Replicate the append operation to other servers if this server is the leader
                    if self.role == 'leader':
                        replication_success = self.replicate_append(file_name, data)

                        # If strong consistency is required, check for replication success
                        if not replication_success:
                            return "append failed on replicas"
                    
                    return "append success"
                else:
                    return "File not found"
            else:
                return "You do not hold the lock"
    
    def replicate_append(self, file_name, data):
        message = f"append_file:{self.server_id}:{file_name}:{data}"
        acknowledgments = 0
        for peer in self.peers:
            try:
                self.sock.sendto(message.encode(), peer)
                self.sock.settimeout(2)  # Timeout for receiving ack
                ack, addr = self.sock.recvfrom(1024)
                if ack.decode() == "append_ack":
                    acknowledgments += 1
            except Exception as e:
                print(f"Error replicating to peer {peer}: {e}")

        # Require majority acknowledgment
        if acknowledgments >= len(self.peers) // 2:
            return True
        else:
            return False

    def monitor_lock_expiration(self):
        while True:
            with self.lock:
                if self.current_lock_holder and self.lock_expiration_time and time.time() > self.lock_expiration_time:
                    print(f"Lock held by {self.current_lock_holder} expired, releasing lock")
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.save_state()  # Save state after lock expiration
            time.sleep(1)
    
    def handle_request(self, data, client_address):
        message = data.decode()
        print(f"[DEBUG] handle_request received message: {message} from {client_address}")

        if message.startswith("acquire_lock"):
            client_id = message.split(":")[1]
            response = self.acquire_lock(client_id)
            self.sock.sendto(response.encode(), client_address)
            print(f"[DEBUG] Sent acquire_lock response: {response}")
            return
        
        elif message.startswith("release_lock"):
            client_id = message.split(":")[1]
            response = self.release_lock(client_id)
            self.sock.sendto(response.encode(), client_address)
            print(f"[DEBUG] Sent release_lock response: {response}")
            return

        elif message.startswith("append_file"):
            client_id = message.split(":")[1]
            file_info = urllib.parse.unquote(message.split(":", 3)[3])
            file_name, file_data = file_info.split(":")
            response = self.append_to_file(client_id, file_name, file_data)
            self.sock.sendto(response.encode(), client_address)
            print(f"[DEBUG] Sent append_file response: {response}")
            return

        elif message == "ping":
            print("[DEBUG] handle_request responding to ping")
            self.sock.sendto("pong".encode(), client_address)
            return
                
        elif message.startswith("identify_leader"):
            if self.role == 'leader':
                response = "I am the leader"
                print("[DEBUG] Server responding as leader")
            else:
                if self.leader_address:
                    leader_host, leader_port = self.leader_address
                    response = f"Redirect to leader:{leader_host}:{leader_port}"
                else:
                    response = "Leader unknown"
            self.sock.sendto(response.encode(), client_address)
            print(f"[DEBUG] Sent identify_leader response: {response}")
            return


    def handle_messages(self):
        self.raft_sock.settimeout(3)
        while True:
            try:
                data, addr = self.raft_sock.recvfrom(1024)
                message = data.decode()
                print(f"[DEBUG] handle_messages received: {message} from {addr}")
                parts = message.split(":")

                if parts[0] == "request_vote":
                    term = int(parts[1])
                    candidate_id = int(parts[2])
                    self.process_vote_request(term, candidate_id, addr)
                elif parts[0] == "vote_granted":
                    with self.lock:  # Add a lock to prevent race conditions
                        self.votes += 1
                        if self.votes > len(self.peers) // 2:
                            # Successfully received majority votes, transition to leader
                            self.role = 'leader'
                            self.leader_address = self.server_address
                            print(f"[DEBUG] Server {self.server_id} became the leader for term {self.term}")
                            self.send_heartbeats()
                elif parts[0] == "new_leader":
                    term = int(parts[1])
                    leader_id = int(parts[2])
                    if term >= self.term:
                        # Step down to follower and acknowledge the new leader
                        self.term = term
                        self.role = 'follower'
                        self.voted_for = None
                        self.leader_address = (addr[0], addr[1] - 1)  # Use addr to derive leader address correctly
                        self.last_heartbeat = time.time()
                        print(f"[DEBUG] Server {self.server_id} acknowledges server {leader_id} as the new leader for term {term}.")

            except socket.timeout:
                continue
            except Exception as e:
                print(f"[DEBUG] Error in handle_messages: {e}")
                continue

    def save_state(self):
        # Load existing history if it exists
        if os.path.exists(self.STATE_FILE):
            with open(self.STATE_FILE, "r") as f:
                try:
                    state_data = json.load(f)
                    history = state_data.get("history", [])
                except json.JSONDecodeError:
                    history = []
        else:
            history = []

        # Create a new record for the current lock state
        new_record = {
            "timestamp": time.time(),
            "current_lock_holder": self.current_lock_holder,
            "lock_expiration_time": self.lock_expiration_time,
            "event": "Lock acquired" if self.current_lock_holder else "Lock released"
        }

        # Append the new record to the history
        history.append(new_record)

        # Save the updated history back to the JSON file
        state = {
            "current_lock_holder": self.current_lock_holder,
            "lock_expiration_time": self.lock_expiration_time,
            "request_history": self.request_history,
            "history": history
        }
        with open(self.STATE_FILE, "w") as f:
            json.dump(state, f, indent=4)  # indent for readability
        print("[DEBUG] Server state with history saved.")

    def heartbeat_check(self):
        while True:
            if self.role == 'follower' and (time.time() - self.last_heartbeat) > self.election_timeout:
                self.start_election()
            time.sleep(0.1)

    def send_heartbeats(self):
        # Send heartbeats periodically if this server is the leader
        while self.role == 'leader':
            message = f"heartbeat:{self.term}:{self.server_id}"
            for peer in self.peers:
                self.raft_sock.sendto(message.encode(), peer)
            time.sleep(0.2)  # Send heartbeat every 0.2 seconds

    def process_heartbeat(self, term, leader_id):
        if term >= self.term:
            self.role = 'follower'
            self.term = term
            self.voted_for = leader_id
            self.last_heartbeat = time.time()
            print(f"[DEBUG] Heartbeat processed from leader {leader_id} for term {term}. Reset election timer.")

    def start_election(self):  # Modified election
        time.sleep(random.uniform(2.0,4.0))  # Increase jitter time significantly to reduce election overlap

        self.role = 'candidate'
        self.term += 1
        self.voted_for = self.server_id
        self.votes = 1  # Initialize votes counter to 1 (self-vote)
        print(f"[DEBUG] Server {self.server_id} starting election for term {self.term}")
        self.election_retries = 0

        for peer in self.peers:
            threading.Thread(target=self.request_vote, args=(peer, self.term, self.server_id)).start()

        time.sleep(7)  # 7 seconds to collect votes

        if self.votes > len(self.peers) // 2:
            self.role = 'leader'
            self.leader_address = self.server_address
            self.election_retries = 0  # Reset retries on successful election
            print(f"[DEBUG]!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print(f"[DEBUG] Server {self.server_id} successfully became leader for term {self.term}")
            print(f"[DEBUG]!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            # Notify all peers that this server is now the leader
            for peer in self.peers:
                self.raft_sock.sendto(f"new_leader:{self.term}:{self.server_id}".encode(), peer)
            self.send_heartbeats()
        else:
            self.role = 'follower'
            self.voted_for = None  # Reset vote for next election attempt
            self.election_retries += 1
            backoff_time = random.uniform(4, 8) * (1 + self.election_retries / 3)  # Increase backoff with retries
            print(f"[DEBUG] Server {self.server_id} remains a follower, backing off for {backoff_time} seconds (Retry {self.election_retries})")
            if self.election_retries < 5:  # Limit retries to avoid endless attempts
                time.sleep(backoff_time)
            else:
                print(f"[DEBUG] Server {self.server_id} backing off significantly due to too many retries.")
                time.sleep(random.uniform(8, 12))  # Significant backoff after repeated failures
                self.election_retries = 0  # Reset retries after significant backoff
        
    def request_vote(self, peer, term, candidate_id):
        message = f"request_vote:{term}:{candidate_id}"
        try:
            self.raft_sock.sendto(message.encode(), peer)
            print(f"[DEBUG] Server {self.server_id} sent vote request to {peer} for term {term}")
            self.raft_sock.settimeout(4)  # Set a timeout to wait for a response
            data, addr = self.raft_sock.recvfrom(1024)
            if data.decode() == "vote_granted":
                self.votes += 1
                print(f"[DEBUG] Server {self.server_id} received vote from {addr}, total votes: {self.votes}")
                return
            else:
                print(f"[DEBUG] Server {self.server_id} did not receive vote from {addr}")
                return
        except socket.timeout:
            print(f"[DEBUG] Server {self.server_id} vote request to {peer} timed out.")
            return
        except Exception as e:
            print(f"[DEBUG] Server {self.server_id} error in request_vote: {e}")
            return

    def process_vote_request(self, term, candidate_id, addr):
        if term > self.term:
            # Update to the new term and vote for the candidate
            self.term = term
            self.role = 'follower'
            self.voted_for = candidate_id
            self.last_heartbeat = time.time()
            self.raft_sock.sendto("vote_granted".encode(), addr)
            print(f"[DEBUG] Vote granted to candidate {candidate_id} from server {self.server_id} for term {term}")
        elif term == self.term and self.voted_for is None:
            # If term is the same, grant vote if not already voted
            self.voted_for = candidate_id
            self.raft_sock.sendto("vote_granted".encode(), addr)
            self.last_heartbeat = time.time()
            print(f"[DEBUG] Vote granted to candidate {candidate_id} from server {self.server_id} for term {term}")
        elif term == self.term and self.voted_for == candidate_id:
            # Re-confirm vote for the candidate if it's already granted in the current term
            self.raft_sock.sendto("vote_granted".encode(), addr)
            print(f"[DEBUG] Reconfirming vote for candidate {candidate_id} from server {self.server_id} for term {term}")
        else:
            print(f"[DEBUG] Vote not granted to candidate {candidate_id} from server {self.server_id} for term {term} - Already voted or term is lower.")
            return
        
    def handle_append_entry(self, term, lock_holder):
        if term >= self.term:
            self.term = term
            self.current_lock_holder = lock_holder
            self.last_heartbeat = time.time()  # Reset election timer
    
    def simulate_crash(self):
        print(f"[DEBUG] Server {self.server_id} is simulating a crash.")
        # Save current state before 'crash'
        self.save_state()
        # Close sockets to stop communication
        self.sock.close()
        self.raft_sock.close()
        # Exit the program to simulate a full crash
        os._exit(1)


if __name__ == "__main__":
    server = LockManagerServer()
    server.start()
