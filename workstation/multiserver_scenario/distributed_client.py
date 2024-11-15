import socket
import time
import threading
import random
from rpc_connection import RPCConnection
import urllib.parse

class DistributedClient:
    def __init__(self, client_id, servers):
        self.client_id = client_id
        self.servers = servers  # List of known server addresses (host, port)
        self.current_leader = None
        self.connection = None  # Will be set when we find the leader
        self.lock_acquired = False
        self.request_id = 0  # Initialize request counter
        self.lease_duration = None

    def next_request_id(self):
        # Generates new request ID per request
        self.request_id += 1
        return str(self.request_id)

    def find_leader(self, retries=5, delay=2):
        for attempt in range(retries):
            print(f"{self.client_id} - Attempting to find leader, attempt {attempt + 1}/{retries}")
            unreachable_servers = set()
            
            for address in self.servers:
                if address in unreachable_servers:
                    continue
                self.connection = RPCConnection(*address)
                response = self.connection.rpc_send(f"identify_leader:{self.client_id}")
                print(f"{self.client_id} - Received response: {response}")
                
                if response == "I am the leader":
                    self.current_leader = address
                    print(f"{self.client_id} - Leader found: {self.current_leader}")
                    time.sleep(1)  # Short delay to let leader fully stabilize
                    return True
                elif response.startswith("Redirect to leader"):
                    # Extract the redirected leader's address and retry finding it
                    parts = response.split(":")
                    if len(parts) == 3:
                        redirected_host = parts[1]
                        redirected_port = int(parts[2])
                        self.current_leader = (redirected_host, redirected_port)
                        print(f"{self.client_id} - Redirected to leader: {self.current_leader}")
                        return True
                elif response == "Leader unknown":
                    # Continue searching without assuming this server is the leader
                    print(f"{self.client_id} - Server at {address} does not know the leader. Continuing search.")
                elif response.startswith("Timeout"):
                    unreachable_servers.add(address)
                    
            # Delay between attempts to prevent flooding and allow time for stabilization
            time.sleep(delay)

        # If retries are exhausted and no leader is found
        self.current_leader = None
        print(f"{self.client_id} - Failed to find leader after {retries} attempts")
        return False

    def send_lease_check(self):
        while True:
            if self.lock_acquired:
                # Add random delay before renewing the lease to reduce collision
                time.sleep(random.uniform(0.5, 1.5))
                # Send lease renewal check if lock is currently held
                response = self.send_message("client_lease_check")
                if response != "lease renewed":
                    print(f"{self.client_id}: Failed to renew lease, response: {response}")
                    self.lock_acquired = False  # Mark lock as lost
                    break
            else:
                # Check if lock has been granted to the client from the queue
                response = self.send_message("client_lease_check")
                if response == "lease renewed":
                    print(f"{self.client_id}: Lock acquired via lease check.")
                    self.lock_acquired = True
                elif response == "Lock is still pending":
                    print(f"{self.client_id}: Waiting for lock to be granted...")
                else:
                    print(f"{self.client_id}: Lock acquisition failed or unexpected response - {response}")
                    break
            # Sleep for a portion of the lease duration if lock held, otherwise check every second
            time.sleep(self.lease_duration / 3 if self.lock_acquired else 1)

    def acquire_lock(self, max_retries=15, base_interval=1):
        retry_interval = base_interval
        for attempt in range(max_retries):
            response = self.send_message("acquire_lock_request")
            if response.startswith("grant lock"):
                self.lock_acquired = True
                _, lease_duration_str = response.split(":")
                self.lease_duration = int(lease_duration_str)
                threading.Thread(target=self.send_lease_check, daemon=True).start()
                print(f"{self.client_id}: Lock acquired with lease duration of {self.lease_duration} seconds.")
                return True
            elif response in ["Request for lock added to queue", "Lock is still pending"]:
                print(f"{self.client_id}: Lock request pending. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval + random.uniform(0.1, 0.5))  # Random backoff
            else:
                print(f"{self.client_id}: Failed to acquire lock - {response}")
                return False
        print(f"{self.client_id}: Lock acquisition failed after retries.")
        return False

    def send_message(self, message_type, additional_info=""):
        # Find leader if not set
        if not self.current_leader and not self.find_leader():
            return "Leader not found"
        
        # Send msg, retry on redirect
        response = self.connection.rpc_send(f"{message_type}:{self.client_id}:{self.next_request_id()}:{urllib.parse.quote(additional_info)}")
        if response.startswith("Redirect to leader"):
            if not self.find_leader():  # Update leader if redirected
                return "Leader redirect failed"
            return self.send_message(message_type, additional_info)  # Retry msg to new leader
        return response


    def start_lease_timer(self):
        expiration_time = time.time() + self.lease_duration
        while time.time() < expiration_time and self.lock_acquired:
            time.sleep(1)
        if self.lock_acquired:
            self.lock_acquired = False  # Mark lock as expired
            print(f"{self.client_id}: Lock lease expired")

    def release_lock(self, client_id):
        with self.lock:  # Thread-safe access
            if self.current_lock_holder == client_id:
                print(f"[DEBUG] Lock released by {client_id}")
                self.current_lock_holder = None
                self.lock_expiration_time = None

                # Grant lock to the next client in queue
                if self.lock_queue:
                    next_client = self.lock_queue.pop(0)  # Remove the first client in queue
                    print(f"[DEBUG] Granting lock to next client in queue: {next_client}")
                    self.current_lock_holder = next_client
                    self.lock_expiration_time = time.time() + LOCK_LEASE_DURATION
                    self.notify_followers_lock_state()
                    # Notify the next client
                    return f"grant lock:{LOCK_LEASE_DURATION}"
                
                self.save_state()
                return "unlock success"
            else:
                print(f"[ERROR] {client_id} tried to release a lock it does not hold.")
                return "You do not hold the lock"

    def send_heartbeat(self):
        while self.lock_acquired:
            response = self.send_message("heartbeat")
            if response != "lease renewed":
                print(f"{self.client_id}: Failed to renew lease, response: {response}")
                self.lock_acquired = False  # Mark lock as lost
                break
            time.sleep(self.lease_duration / 3)

    def append_file(self, file_name, data):
        # Appends data to file if lock held
        if self.lock_acquired:
            response = self.send_message("append_file", f"{file_name}:{data}")
            
            if response == "append success":
                print(f"{self.client_id}: Data appended to {file_name}")
                return "append success"
            elif response == "append failed on replicas":
                print(f"{self.client_id}: Append succeeded locally but failed on replicas.")
                return "replication failed"
            else:
                print(f"{self.client_id}: Append failed or no response - {response}")
                return response
        else:
            print(f"{self.client_id}: Cannot append to file - lock not held.")
            return "lock not held"

    def close(self):
        # Closes connection
        if self.connection:
            self.connection.close()
        print(f"{self.client_id}: Connection closed.")

def client_task(client_id, servers, file_name, data):
    """
    Task for a client to acquire a lock, append data, and release the lock.
    """
    client = DistributedClient(client_id, servers)
    try:
        print(f"[{client_id}] Starting task...")
        
        # Step 1: Find the leader
        if not client.find_leader():
            print(f"[{client_id}] Failed to find a leader. Exiting.")
            return
        
        # Step 2: Attempt to acquire the lock
        print(f"[{client_id}] Attempting to acquire the lock...")
        if client.acquire_lock():
            print(f"[{client_id}] Lock acquired successfully.")
            
            # Step 3: Append data to the file
            print(f"[{client_id}] Attempting to append '{data}' to {file_name}...")
            append_result = client.append_file(file_name, data)
            if append_result == "append success":
                print(f"[{client_id}] Data '{data}' appended successfully.")
            elif append_result == "replication failed":
                print(f"[{client_id}] Data '{data}' appended locally but failed to replicate to replicas.")
            else:
                print(f"[{client_id}] Append operation failed: {append_result}")
            
            # Step 4: Release the lock
            print(f"[{client_id}] Releasing the lock...")
            client.release_lock()
            print(f"[{client_id}] Lock released successfully.")
        else:
            print(f"[{client_id}] Failed to acquire the lock.")
    
    except Exception as e:
        print(f"[{client_id}] Exception occurred: {e}")
    
    finally:
        # Step 5: Close the connection
        print(f"[{client_id}] Closing connection...")
        client.close()
        print(f"[{client_id}] Task completed.")

def test_simultaneous_clients():
    # Define the server addresses
    servers = [('localhost', 8080), ('localhost', 8081), ('localhost', 8082)]
    
    # File and data to append
    file_name = "file_1"
    client_1_data = "A"
    client_2_data = "B"
    
    # Create threads for both clients
    client_1_thread = threading.Thread(target=client_task, args=("client_1", servers, file_name, client_1_data))
    client_2_thread = threading.Thread(target=client_task, args=("client_2", servers, file_name, client_2_data))
    
    # Start both threads simultaneously
    client_1_thread.start()
    client_2_thread.start()
    
    # Wait for both threads to complete
    client_1_thread.join()
    client_2_thread.join()
    print("[TEST] Both clients have completed their tasks.")

# Run the test
if __name__ == "__main__":
    test_simultaneous_clients()