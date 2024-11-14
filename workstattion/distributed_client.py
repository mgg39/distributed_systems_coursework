import socket
import time
import threading
from rpc_connection import RPCConnection
import urllib.parse

class DistributedClient:
    def __init__(self, client_id, replicas):
        self.client_id = client_id
        self.replicas = replicas  # List of known server addresses (host, port)
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
            
            for address in self.replicas:
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

    def acquire_lock(self, max_retries=5, base_interval=0.5):  # shorter base interval
        retry_interval = base_interval
        for attempt in range(max_retries):
            response = self.send_message("acquire_lock")
            if response.startswith("grant lock"):
                self.lock_acquired = True
                _, lease_duration_str = response.split(":")
                self.lease_duration = int(lease_duration_str)
                threading.Thread(target=self.start_lease_timer, daemon=True).start()
                return True
            elif response == "Lock is currently held":
                print(f"{self.client_id}: Lock is held, retrying...")
            time.sleep(retry_interval)
            retry_interval *= 2  # Still exponential but starts with shorter interval
        return False


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
                return "append success"  # Ensure success response is returned
            else:
                print(f"{self.client_id}: Append failed or no response - {response}")
                return response  # Return actual response in case of failure
        else:
            print(f"{self.client_id}: Cannot append to file - lock not held.")
            return "lock not held"

    def close(self):
        # Closes connection
        if self.connection:
            self.connection.close()
        print(f"{self.client_id}: Connection closed.")

"""
# Example usage
if __name__ == "__main__":
    # list of known server addresses
    replicas = [('localhost', 8080), ('localhost', 8081), ('localhost', 8082)]
    client = DistributedClient("client_1", replicas)
    
    try:
        # Try to acquire lock with retries
        if client.acquire_lock():
            # Append data to file if lock acquired
            client.append_file("file_1", "This is a test log entry.")
    finally:
        # Ensure lock is released after operations are complete
        client.release_lock()
        # Close the connection
        client.close()
"""