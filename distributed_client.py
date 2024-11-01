import time
from rpc_connection import RPCConnection

class DistributedClient:
    def __init__(self, client_id, server_address='localhost', server_port=8080):
        self.client_id = client_id
        self.connection = RPCConnection(server_address, server_port)
        self.lock_acquired = False
        self.request_id = 0  # Initialize request counter

    def next_request_id(self):
        # Generates new request ID per request
        self.request_id += 1
        return str(self.request_id)

    def send_message(self, message_type, additional_info=""):
        # Send message with request ID & handle response
        request_id = self.next_request_id()
        message = f"{message_type}:{self.client_id}:{request_id}:{additional_info}"
        response = self.connection.rpc_send(message)
        return response

    def acquire_lock(self, max_retries=5, base_interval=1):
        # Attempt to acquire lock (increasing wait time between attempts)
        retry_interval = base_interval
        for attempt in range(max_retries):
            response = self.send_message("acquire_lock")
            
            if response == "grant lock":
                self.lock_acquired = True
                print(f"{self.client_id}: Lock acquired successfully.")
                return True  # Lock acquired
            
            elif response == "Lock is currently held":
                print(f"{self.client_id}: Lock is held, retrying...")
            
            else:
                print(f"{self.client_id}: No response or unexpected response, retrying...")
            
            # Exponential backoff
            time.sleep(retry_interval)
            retry_interval *= 2  # Double the wait time for the next retry

        print(f"{self.client_id}: Failed to acquire lock after {max_retries} attempts.")
        return False  # Lock not acquired

    def release_lock(self):
        # Releases lock if held
        if self.lock_acquired:
            response = self.send_message("release_lock")
            if response == "unlock success":
                self.lock_acquired = False
                print(f"{self.client_id}: Lock released successfully.")
            else:
                print(f"{self.client_id}: Failed to release lock - {response}")
        else:
            print(f"{self.client_id}: Cannot release lock - lock not held by this client.")

    def append_file(self, file_name, data):
        #Appends data to file if lock held
        if self.lock_acquired:
            response = self.send_message(f"append_file:{file_name}:{data}")
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
"""
if __name__ == "__main__":
    client = DistributedClient("client_1")
    
    try:
        # Try to acquire lock with retries
        if client.acquire_lock():
            # Append data to file if lock acquired
            client.append_file("file_0", "This is a test log entry.")
    finally:
        # Ensure lock is released after operations are complete
        client.release_lock()
        # Close the connection
        client.close()
"""