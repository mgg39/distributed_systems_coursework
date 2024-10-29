from rpc_connection import RPCConnection
import time

class DistributedClient:
    def __init__(self, client_id, server_address='localhost', server_port=8080):
        self.client_id = client_id
        self.connection = RPCConnection(server_address, server_port)
        self.lock_acquired = False

    def send_message(self, message):
        response = self.connection.rpc_send(message)
        if response:
            print(f"{self.client_id}: {response}")
        else:
            print(f"{self.client_id}: No response from server (timeout)")
        return response

    def acquire_lock(self, max_retries=5, retry_interval=1):
        #Attempts to acquire lock, with retries
        for attempt in range(max_retries):
            response = self.send_message("acquire_lock")
            if response == "Lock acquired":
                self.lock_acquired = True
                print(f"{self.client_id}: Lock acquired successfully.")
                return
            elif response == "Lock is currently held":
                print(f"{self.client_id}: Lock is held, retrying...")
                time.sleep(retry_interval)  # Wait and retry
            else:
                print(f"{self.client_id}: Unexpected response or timeout.")
                time.sleep(retry_interval)

        print(f"{self.client_id}: Failed to acquire lock after {max_retries} attempts.")

    def release_lock(self):
        #Releases lock if held
        if self.lock_acquired:
            response = self.send_message("release_lock")
            if response == "Lock released":
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
            print(f"{self.client_id}: AppendFile Response: {response}")
        else:
            print(f"{self.client_id}: Cannot append to file - lock not held.")

    def close(self):
        #Closes connection to server
        self.connection.close()
        print(f"{self.client_id}: Connection closed.")

# Example usage
"""
if __name__ == "__main__":
    client = DistributedClient("client_1")
    
    client.acquire_lock()
    
    client.append_file("file_0", "This is a test log entry.")
    
    client.release_lock()
    
    client.close()
"""