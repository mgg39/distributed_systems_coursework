import socket
import time
import threading
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
            # Wait for a third of the lease duration if lock held, otherwise check every second
            time.sleep(self.lease_duration / 3 if self.lock_acquired else 1)

    def acquire_lock(self, max_retries=5, base_interval=0.5):
        retry_interval = base_interval
        for attempt in range(max_retries):
            response = self.send_message("acquire_lock_request")
            if response.startswith("grant lock"):
                self.lock_acquired = True
                _, lease_duration_str = response.split(":")
                self.lease_duration = int(lease_duration_str)
                threading.Thread(target=self.send_lease_check, daemon=True).start()  # Start lease checks
                print(f"{self.client_id}: Lock acquired with lease duration of {self.lease_duration} seconds.")
                return True
            elif "Request for lock" in response:
                print(f"{self.client_id}: Lock request added to the queue.")
                threading.Thread(target=self.send_lease_check, daemon=True).start()  # Start lease checks in queue
                return True
            elif response == "Lock is currently held":
                print(f"{self.client_id}: Lock is held, retrying after {retry_interval} seconds...")
            else:
                print(f"{self.client_id}: Failed to acquire lock - {response}")
            time.sleep(retry_interval)
            retry_interval *= 2
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


    def release_lock(self):
        if self.lock_acquired:
            response = self.send_message("release_lock")
            if response == "unlock success":
                self.lock_acquired = False
                print(f"{self.client_id}: Lock released successfully.")
            else:
                print(f"{self.client_id}: Failed to release lock - {response}")
        else:
            print(f"{self.client_id}: Cannot release lock - lock not held by this client.")

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

def test_distributed_client():
    # Step 1: Define the server addresses
    servers = [('localhost', 8080), ('localhost', 8081), ('localhost', 8082)]
    client = DistributedClient("client_1", servers)
    
    try:
        print("[TEST] Starting distributed client test...")
        
        # Step 2: Find the leader
        if not client.find_leader():
            print("[TEST] Failed to find a leader. Test aborted.")
            return
        
        # Step 3: Acquire the lock
        print("[TEST] Attempting to acquire the lock...")
        if client.acquire_lock():
            print("[TEST] Lock acquired successfully.")
        else:
            print("[TEST] Failed to acquire the lock after retries. Test aborted.")
            return
        
        # Step 4: Start lease check in the background
        print("[TEST] Starting lease check in the background...")
        lease_check_thread = threading.Thread(target=client.send_lease_check, daemon=True)
        lease_check_thread.start()

        # Step 5: Append data to a file
        print("[TEST] Attempting to append data to file...")
        append_result = client.append_file("file_1", "This is a test log entry.")
        if append_result == "append success":
            print("[TEST] Data appended successfully.")
        elif append_result == "replication failed":
            print("[TEST] Data appended locally but failed to replicate to replicas.")
        else:
            print(f"[TEST] Append operation failed with response: {append_result}")
        
        # Step 6: Release the lock
        print("[TEST] Releasing the lock...")
        client.release_lock()
        print("[TEST] Lock released successfully.")
    
    except Exception as e:
        print(f"[TEST] Exception occurred: {e}")
    
    finally:
        # Step 7: Close the connection
        print("[TEST] Closing the client connection...")
        client.close()
        print("[TEST] Client shut down.")

# Run the test
if __name__ == "__main__":
    test_distributed_client()