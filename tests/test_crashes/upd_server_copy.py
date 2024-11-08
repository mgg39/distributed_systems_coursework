import socket
import os
import threading
import time
from collections import defaultdict
import urllib.parse
import json

FILES_DIR = "server_files"
NUM_FILES = 100
LOCK_LEASE_DURATION = 10  # Lease duration in seconds
STATE_FILE = "server_state.json"

# Ensure the files directory exists and create 100 files
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)
for i in range(NUM_FILES):
    open(os.path.join(FILES_DIR, f"file_{i}"), 'a').close()

class LockManagerServer:
    def __init__(self, host='localhost', port=8080):
        self.server_address = (host, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(self.server_address)
        self.lock = threading.Lock()
        self.current_lock_holder = None
        self.lock_expiration_time = None  # Stores when the lock should expire
        self.request_history = defaultdict(dict)  # Stores each client's request history
        self.load_state() # Load any existing state server

        # Start background thread to monitor lock expiration
        threading.Thread(target=self.monitor_lock_expiration, daemon=True).start() #client  crash protection

    def start(self):
        print(f"Server listening on {self.server_address}")
        while True:
            data, client_address = self.sock.recvfrom(1024)
            threading.Thread(target=self.handle_request, args=(data, client_address)).start()

    def acquire_lock(self, client_id):
        # Attempt to acquire lock and set lease expiration if successful
        with self.lock:
            current_time = time.time()
            if self.current_lock_holder is None or (self.lock_expiration_time and current_time > self.lock_expiration_time):
                self.current_lock_holder = client_id  # Set current_lock_holder
                self.lock_expiration_time = current_time + LOCK_LEASE_DURATION  # Set lock_expiration_time
                print(f"[DEBUG] {client_id} acquired the lock with expiration {self.lock_expiration_time}")
                self.save_state()  # Save state after setting variables
                return f"grant lock:{LOCK_LEASE_DURATION}"
            else:
                return "Lock is currently held"

            
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
        #Renew lease if client holding lock sends a heartbeat
        with self.lock:
            if self.current_lock_holder == client_id:
                self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                print(f"Lease renewed by {client_id}. New expiration time: {self.lock_expiration_time}")
                return "lease renewed"
            else:
                return "You do not hold the lock"

    def append_to_file(self, client_id, file_name, data):
        # Append data to file if lock is held by the requesting client
        with self.lock:
            if self.current_lock_holder == client_id:
                file_path = os.path.join(FILES_DIR, file_name)
            
                # Debugging
                #print(f"[DEBUG] Client {client_id} holds the lock and is attempting to append to {file_path}.")
                #print(f"[DEBUG] Data to append: '{data}'")
                
                # Check if file exists before writing
                if os.path.exists(file_path):
                    with open(file_path, 'a') as f:
                        f.write(data + "\n")
                        f.flush()  # Flush internal buffer
                        os.fsync(f.fileno())  # Force write to disk
                    print(f"[DEBUG] Data appended successfully to {file_name}.")
                    return "append success"
                else:
                    print(f"[DEBUG] File {file_name} not found in {FILES_DIR}.")
                    return "File not found"
            else:
                print(f"[DEBUG] Client {client_id} does not hold the lock. Cannot append.")
                return "You do not hold the lock"

    def monitor_lock_expiration(self):
        #Background task to check and release expired locks
        while True:
            with self.lock:
                if self.current_lock_holder and self.lock_expiration_time and time.time() > self.lock_expiration_time:
                    print(f"Lock held by {self.current_lock_holder} has expired. Releasing lock.")
                    self.current_lock_holder = None
                    self.lock_expiration_time = None
                    self.save_state()  # Save state after automatic release due to expiration
            time.sleep(1)  # Check every second

        
    def handle_request(self, data, client_address):
        message = data.decode()
        print(f"Handling message: {message} from {client_address}")

        # Initialize response (avoid AttributeError)
        response = "Invalid request format"

        try:
            # Split the message into components for request type, client ID, and request ID
            parts = message.split(":", 3)
            request_type = parts[0]
            client_id = parts[1]
            request_id = parts[2]

            # Handle append_file request separately due to additional information
            if request_type == "append_file" and len(parts) == 4:
                additional_info = urllib.parse.unquote(parts[3])
                try:
                    # Split additional_info to extract file_name and file_data
                    file_name, file_data = additional_info.split(":", 1)
                    response = self.append_to_file(client_id, file_name, file_data)
                except ValueError:
                    response = "Invalid append_file format"
            else:
                # Process other request types and update response accordingly
                response = self.process_other_request_types(request_type, client_id, request_id)
        except ValueError:
            # This will be used if any parsing error occurs
            response = "Invalid request format"

        # Ensure response is sent back to the client
        print(f"[DEBUG] Sending response to {client_id}: {response}")
        self.sock.sendto(response.encode(), client_address)
        
    def process_other_request_types(self, request_type, client_id, request_id): #other client commands to server for lock managing
        if request_type == "acquire_lock":
            return self.acquire_lock(client_id)
        elif request_type == "release_lock":
            return self.release_lock(client_id)
        elif request_type == "heartbeat":
            return self.renew_lease(client_id)
        else:
            return "Unknown command"
    
    def save_state(self):
        # Load existing history if it exists
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
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
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=4)  # indent for readability
        print("[DEBUG] Server state with history saved.")

    def load_state(self):
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            self.current_lock_holder = state.get("current_lock_holder")
            self.lock_expiration_time = state.get("lock_expiration_time")
            self.request_history = state.get("request_history", defaultdict(dict))
            print("[DEBUG] Server state loaded.")
        
if __name__ == "__main__":
    server = LockManagerServer()
    server.start()
    