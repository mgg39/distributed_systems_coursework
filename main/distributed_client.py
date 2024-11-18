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
                    parts = response.split(":")
                    if len(parts) == 3:
                        redirected_host = parts[1]
                        redirected_port = int(parts[2])
                        self.current_leader = (redirected_host, redirected_port)
                        print(f"{self.client_id} - Redirected to leader: {self.current_leader}")
                        return True
                elif response == "Leader unknown":
                    print(f"{self.client_id} - Server at {address} does not know the leader. Continuing search.")
                elif response.startswith("Timeout"):
                    unreachable_servers.add(address)
                    
            time.sleep(delay)

        self.current_leader = None
        print(f"{self.client_id} - Failed to find leader after {retries} attempts")
        return False

    def check_queue_status(self):
        response = self.send_message("get_queue_status")
        if response.startswith("queued"):
            position = int(response.split(":")[1].replace("position_", ""))
            print(f"{self.client_id}: Currently in queue at position {position}.")
            return position
        elif response == "You hold the lock":
            print(f"{self.client_id}: Lock already held by this client.")
            self.lock_acquired = True
            return -1  # Lock is already held
        else:
            print(f"{self.client_id}: Unexpected queue status response: {response}")
            return None  # Unexpected response

    def acquire_lock(self, max_wait_time=60, check_interval=5):
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            response = self.send_message("acquire_lock")
            if response.startswith("grant lock"):
                # Lock granted
                self.lock_acquired = True
                _, lease_duration_str = response.split(":")
                self.lease_duration = int(lease_duration_str)
                threading.Thread(target=self.start_lease_timer, daemon=True).start()
                print(f"{self.client_id}: Lock acquired with lease duration of {self.lease_duration} seconds.")
                return True
            elif response.startswith("queued"):
                # Added to queue, extract position
                position = int(response.split(":")[1].replace("position_", ""))
                print(f"{self.client_id}: Added to queue at position {position}. Retrying later...")
                time.sleep(check_interval)  # Wait before checking again
            elif response.startswith("Redirect to leader"):
                # Redirect to new leader
                self.find_leader()
                continue
            else:
                # Unexpected response
                print(f"{self.client_id}: Unexpected response - {response}")

            # Check timeout
            if time.time() - start_time >= max_wait_time:
                print(f"{self.client_id}: Waited too long in queue. Giving up.")
                return False

        print(f"{self.client_id}: Exceeded maximum wait time of {max_wait_time} seconds.")
        return False

    def start_lease_timer(self):
        expiration_time = time.time() + self.lease_duration
        while time.time() < expiration_time and self.lock_acquired:
            time.sleep(1)
        if self.lock_acquired:
            self.lock_acquired = False
            print(f"{self.client_id}: Lock lease expired")

    def release_lock(self):
        if self.lock_acquired:
            for attempt in range(3):  # Retry release lock up to 3 times
                response = self.send_message(f"release_lock:{self.client_id}")
                
                if response == "unlock success":
                    self.lock_acquired = False
                    print(f"{self.client_id}: Lock released successfully.")
                    return
                
                elif response == "You do not hold the lock":
                    print(f"{self.client_id}: Lock release failed - client does not hold the lock.")
                    return
                
                else:
                    print(f"{self.client_id}: Failed to release lock, retrying... (Attempt {attempt + 1}/3)")
                    time.sleep(1)  # Short delay before retry
            
            print(f"{self.client_id}: Failed to release lock after retries.")
        else:
            print(f"{self.client_id}: Cannot release lock - lock not held by this client.")

    def send_heartbeat(self):
        while self.lock_acquired:
            response = self.send_message("heartbeat")
            if response != "lease renewed":
                print(f"{self.client_id}: Failed to renew lease, response: {response}")
                self.lock_acquired = False
                break
            time.sleep(self.lease_duration / 3)
    
    def send_message(self, message_type, additional_info=""):
        if not self.current_leader and not self.find_leader():
            return "Leader not found"

        # Handle append_file specifically without URL encoding
        if message_type == "append_file":
            message = f"{message_type}:{self.client_id}:{self.next_request_id()}:{additional_info}"
        else:
            message = f"{message_type}:{self.client_id}:{self.next_request_id()}:{urllib.parse.quote(additional_info)}"
        
        try:
            response = self.connection.rpc_send(message)
            if response.startswith("Redirect to leader"):
                if not self.find_leader():
                    return "Leader redirect failed"
                return self.send_message(message_type, additional_info)  # Retry with updated leader
            return response

        except socket.error as e:
            print(f"Socket error during send: {e}")
            # Attempt to reconnect or handle socket closure
            self.close()
            if not self.find_leader():
                return "Leader search failed"
            return self.send_message(message_type, additional_info)  # Retry the message sending


    def append_file(self, file_name, data):
        if self.lock_acquired:
            # Format the message with file_name and data
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
        if self.connection:
            self.connection.close()
        print(f"{self.client_id}: Connection closed.")

"""
if __name__ == "__main__":
    # List of known server addresses
    servers = [('localhost', 8080)]#, ('localhost', 8082), ('localhost', 8084)]

    # Create two clients
    client1 = DistributedClient("client_1", servers)
    client2 = DistributedClient("client_2", servers)
    client3 = DistributedClient("client_3", servers)

    def client_task(client, file_name, data):
        try:
            print(f"{client.client_id}: Attempting to acquire lock...")

            # Try to acquire lock with a maximum wait time of 60 seconds
            if client.acquire_lock(max_wait_time=60):
                print(f"{client.client_id}: Lock acquired. Attempting to append data to file.")

                # Append data to the file if the lock is acquired
                append_result = client.append_file(file_name, data)

                if append_result == "append success":
                    print(f"{client.client_id}: Data appended successfully.")
                elif append_result == "replication failed":
                    print(f"{client.client_id}: Data appended locally but replication to replicas failed.")
                else:
                    print(f"{client.client_id}: Append failed with response: {append_result}")
            else:
                print(f"{client.client_id}: Failed to acquire lock after waiting.")
        finally:
            print(f"{client.client_id}: Releasing lock if held and closing connection.")
            # Ensure lock is released after operations are complete
            client.release_lock()
            # Close the connection
            client.close()

    # Create threads for each client to simulate simultaneous operations
    thread1 = threading.Thread(target=client_task, args=(client1, "file_1", "1"))
    thread2 = threading.Thread(target=client_task, args=(client2, "file_1", "2"))
    thread3 = threading.Thread(target=client_task, args=(client3, "file_1", "3"))

    # Start the threads
    thread1.start()
    thread2.start()
    thread3.start()

    # Wait for threads to finish
    thread1.join()
    thread2.join()
    thread3.join()

    print("All clients have completed their operations.")
    """
