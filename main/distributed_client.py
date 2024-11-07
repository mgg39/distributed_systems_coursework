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

    def send_message(self, message_type, additional_info=""):
        # Ensure we are connected to the current leader
        if not self.current_leader:
            if not self.find_leader():
                print(f"{self.client_id}: Unable to find leader.")
                return "Leader not found"

        # URL encode additional information for requests
        additional_info_encoded = urllib.parse.quote(additional_info)
        request_id = self.next_request_id()
        if additional_info_encoded:
            message = f"{message_type}:{self.client_id}:{request_id}:{additional_info_encoded}"
        else:
            message = f"{message_type}:{self.client_id}:{request_id}"

        # Send the message to the leader and handle redirection
        response = self.connection.rpc_send(message)
        if response.startswith("Redirect to leader"):
            # Update leader and retry
            host, port = response.split(":")[2:]
            self.current_leader = (host, int(port))
            self.connection = RPCConnection(*self.current_leader)
            print(f"{self.client_id}: Redirected to new leader at {self.current_leader}")
            response = self.connection.rpc_send(message)  # Retry with the new leader
        return response

    def acquire_lock(self, max_retries=5, base_interval=1):
        retry_interval = base_interval
        for attempt in range(max_retries):
            response = self.send_message("acquire_lock")

            if response.startswith("grant lock"):
                self.lock_acquired = True
                _, lease_duration_str = response.split(":")
                self.lease_duration = int(lease_duration_str)
                print(f"{self.client_id}: Lock acquired with lease duration of {self.lease_duration} seconds.")
                threading.Thread(target=self.send_heartbeat, daemon=True).start()
                return True
            elif response == "Lock is currently held":
                print(f"{self.client_id}: Lock is held, retrying...")
            else:
                print(f"{self.client_id}: Unexpected response or no response, retrying...")

            time.sleep(retry_interval)
            retry_interval *= 2
        print(f"{self.client_id}: Failed to acquire lock after {max_retries} attempts.")
        return False

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
        # Send periodic heartbeat to keep lock alive based on lease duration
        if self.lease_duration:
            while self.lock_acquired:
                self.send_message("heartbeat")
                time.sleep(self.lease_duration / 2)  # Send heartbeat halfway through the lease

    def append_file(self, file_name, data):
        # Appends data to file if lock held
        if self.lock_acquired:
            response = self.send_message("append_file", f"{file_name}:{data}")
            if response == "append success":
                print(f"{self.client_id}: Data appended to {file_name}")
            else:
                print(f"{self.client_id}: Append failed or no response - {response}")
        else:
            print(f"{self.client_id}: Cannot append to file - lock not held.")

    def close(self):
        # Closes connection
        if self.connection:
            self.connection.close()
        print(f"{self.client_id}: Connection closed.")

    def find_leader(self):
        for address in self.replicas:
            self.connection = RPCConnection(*address)
            response = self.connection.rpc_send(f"identify_leader:{self.client_id}")

            if response == "I am the leader":
                print(f"[DEBUG] Leader identified at {address}")
                self.current_leader = address
                return True
            elif response.startswith("Redirect to leader"):
                # Extract leader's address if provided in the response
                host, port = response.split(":")[2:]
                self.current_leader = (host, int(port))
                print(f"[DEBUG] Redirected to leader at {self.current_leader}")
                self.connection = RPCConnection(*self.current_leader)
                return True
        return False


"""
# Example usage
if __name__ == "__main__":
    # Provide list of known server addresses
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
