'''
This file simulates the following scenarios:
- Client Timeout: client fails to send heartbeat messages, letting lock expire
- Delayed Message: server handles out-of-order or delayed messages, ensuring that server does not incorrectly extend expired locks
'''

import socket
import time
import threading
from rpc_connection_copy import RPCConnection
import urllib.parse

class DistributedClient:
    def __init__(self, client_id, server_address='localhost', server_port=8080):
        self.client_id = client_id
        self.connection = RPCConnection(server_address, server_port)
        self.lock_acquired = False
        self.request_id = 0  # Initialize request counter
        self.lease_duration = None

    def next_request_id(self):
        # Generates new request ID per request
        self.request_id += 1
        return str(self.request_id)

    def send_message(self, message_type, additional_info=""):
            # URL encode additional 
            additional_info_encoded = urllib.parse.quote(additional_info)

            # Construct the message
            request_id = self.next_request_id()
            if additional_info_encoded:
                message = f"{message_type}:{self.client_id}:{request_id}:{additional_info_encoded}"
            else:
                message = f"{message_type}:{self.client_id}:{request_id}"
            
            response = self.connection.rpc_send(message)
            return response

    def acquire_lock(self, max_retries=5, base_interval=1):
        # Attempt to acquire lock (using self.client_id directly)
        retry_interval = base_interval
        for attempt in range(max_retries):
            response = self.send_message("acquire_lock")
            
            # Check if lock was granted and extract lease duration
            if response.startswith("grant lock"):
                self.lock_acquired = True
                _, lease_duration_str = response.split(":")
                self.lease_duration = int(lease_duration_str)
                print(f"{self.client_id}: Lock acquired with lease duration of {self.lease_duration} seconds.")
                
                # Start heartbeat thread to renew the lease
                threading.Thread(target=self.send_heartbeat, daemon=True).start()
                return True
            
            elif response == "Lock is currently held":
                print(f"{self.client_id}: Lock is held, retrying...")
            
            else:
                print(f"{self.client_id}: No response or unexpected response, retrying...")
            
            time.sleep(retry_interval)
            retry_interval *= 2  # Exponential backoff

        print(f"{self.client_id}: Failed to acquire lock after {max_retries} attempts.")
        return False

    def release_lock(self):
        # Releases lock if held (using self.client_id directly)
        if self.lock_acquired:
            response = self.send_message("release_lock")
            if response == "unlock success":
                self.lock_acquired = False
                print(f"{self.client_id}: Lock released successfully.")
            else:
                print(f"{self.client_id}: Failed to release lock - {response}")
        else:
            print(f"{self.client_id}: Cannot release lock - lock not held by this client.")

    def send_heartbeat(self, simulate_network_issue=False, delay_heartbeat=False): #Client Timeout #Delayed Message
        # Send periodic heartbeat to keep lock alive
        if self.lease_duration:
            while self.lock_acquired:
                if simulate_network_issue:
                    print(f"{self.client_id}: Simulating network failure, heartbeat skipped.")
                    time.sleep(self.lease_duration)  # Wait for entire lease duration to simulate timeout
                elif delay_heartbeat:
                    print(f"{self.client_id}: Introducing artificial delay before sending heartbeat.")
                    time.sleep(self.lease_duration * 1.5)  # Delay beyond the lease duration
                else:
                    self.send_message("heartbeat")
                    print(f"{self.client_id}: Heartbeat sent.")
                time.sleep(self.lease_duration / 2)  # Send heartbeat halfway through the lease


    def append_file(self, file_name, data):
        # Appends data to file if lock held
        if self.lock_acquired:
            # Send message as `append_file:file_name:data` with request ID
            response = self.send_message("append_file", f"{file_name}:{data}")
            if response == "append success":
                print(f"{self.client_id}: Data appended to {file_name}")
            else:
                print(f"{self.client_id}: Append failed or no response - {response}")
        else:
            print(f"{self.client_id}: Cannot append to file - lock not held.")

    def close(self):
        #Closes connection 
        self.connection.close()
        print(f"{self.client_id}: Connection closed.")

# Example usage

if __name__ == "__main__":
    client = DistributedClient("client_1")
    
    try:
        # Try to acquire lock with retries
        if client.acquire_lock():
            # Append data to file if lock acquired
            client.append_file("file_1", "This is a test log entry.")
    finally:
        # Ensure lock is released after operations are complete
        #client.release_lock()  # <--- Comment out when testing client_reconnect_after_server_restart to prevent the client from releasing the lock
        # Close the connection
        client.close()
