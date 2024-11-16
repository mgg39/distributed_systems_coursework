import socket
import time
import threading
from rpc_connection import RPCConnection
import urllib.parse

class DistributedClient:
    def __init__(self, client_id, replicas):
        self.client_id = client_id
        self.servers = servers  # List of known server addresses (host, port)
        self.current_leader = None
        self.connection = None  # Will be set when we find the leader
        self.lock_acquired = False
        self.request_id = 0  # Initialize request counter
        self.lease_duration = None
        self.start_message_listener()

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
                    print(f"{self.client_id}: No leader identified. Will retry later.")
                    return False
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

    def acquire_lock(self, max_wait_time=120):
        if self.lock_acquired:
            print(f"{self.client_id}: Lock already acquired.")
            return True
        if hasattr(self, 'is_waiting') and self.is_waiting:
            print(f"{self.client_id}: Already waiting for lock, skipping redundant requests.")
            return False

        self.is_waiting = True
        try:
            return self._acquire_lock_with_retry(max_wait_time)
        finally:
            self.is_waiting = False

    def _acquire_lock_with_retry(self, max_wait_time):
        start_time = time.time()
        delay = 1  # Initial retry delay
        while time.time() - start_time < max_wait_time:
            response = self.send_message("acquire_lock")
            if response.startswith("grant lock"):
                self.lock_acquired = True
                _, lease_duration_str = response.split(":")
                self.lease_duration = int(lease_duration_str)
                threading.Thread(target=self.start_lease_timer, daemon=True).start()
                print(f"{self.client_id}: Lock acquired with lease duration {self.lease_duration} seconds.")
                return True
            elif response.startswith("queued"):
                position = int(response.split(":")[1].replace("position_", ""))
                print(f"{self.client_id}: Added to queue at position {position}. Retrying after {delay} seconds...")
                time.sleep(delay)
                delay = min(delay * 2, 10)  # Exponential backoff with max delay of 10 seconds
            elif response.startswith("Redirect to leader"):
                print(f"{self.client_id}: Redirected to new leader. Updating leader info.")
                self.find_leader()
                delay = 1  # Reset delay after leader update
            else:
                print(f"{self.client_id}: Unexpected response - {response}")
                return False

            if time.time() - start_time >= max_wait_time:
                print(f"{self.client_id}: Timeout exceeded while waiting for lock.")
                return False
        return False

    def start_lease_timer(self):
        expiration_time = time.time() + self.lease_duration
        while time.time() < expiration_time and self.lock_acquired:
            time.sleep(1)
        if self.lock_acquired:
            self.lock_acquired = False
            print(f"{self.client_id}: Lock lease expired")

    def send_message(self, message_type, additional_info=""):
        print(f"[DEBUG] Sending message: {message_type} with info: {additional_info}")
        if not self.current_leader and not self.find_leader():
            return "Leader not found"
        
        # Ensure connection is established to current leader
        if not self.connection or self.connection.server_address != self.current_leader:
            print(f"[DEBUG] Connection missing or outdated. Reinitializing connection to leader: {self.current_leader}")
            try:
                try:
                    self.connection = RPCConnection(*self.current_leader)
                    print(f"[DEBUG] Connection successfully initialized to leader at {self.current_leader}.")
                except Exception as e:
                    print(f"[ERROR] Failed to initialize connection to leader: {e}")
                    raise
            except Exception as e:
                print(f"[ERROR] Failed to initialize RPC connection: {e}")
        
        try:
            if message_type == "append_file":
                message = f"{message_type}:{self.client_id}:{self.next_request_id()}:{additional_info}"
            else:
                message = f"{message_type}:{self.client_id}:{self.next_request_id()}:{urllib.parse.quote(additional_info)}"
            
            response = self.connection.rpc_send(message)
            
            if response.startswith("Redirect to leader"):
                if not self.find_leader():
                    return "Leader redirect failed"
                return self.send_message(message_type, additional_info)
            
            print(f"[DEBUG] Response received for {message_type}: {response}")
            return response
        except Exception as e:
            print(f"{self.client_id}: Error sending message: {e}")
            # Try to reconnect once
            try:
                try:
                    self.connection = RPCConnection(*self.current_leader)
                    print(f"[DEBUG] Connection successfully initialized to leader at {self.current_leader}.")
                except Exception as e:
                    print(f"[ERROR] Failed to initialize connection to leader: {e}")
                    raise
            except Exception as e:
                print(f"[ERROR] Failed to initialize RPC connection: {e}")
            return f"Error: {str(e)}"

    def release_lock(self):
        if self.lock_acquired:
            for attempt in range(3):
                try:
                    print(f"{self.client_id}: Attempting to release lock (attempt {attempt + 1})")
                    response = self.send_message("release_lock")
                    
                    if response == "unlock success":
                        self.lock_acquired = False
                        self.lease_duration = None
                        print(f"{self.client_id}: Lock released successfully")
                        return True
                    elif response == "You do not hold the lock":
                        self.lock_acquired = False
                        print(f"{self.client_id}: Lock was already released")
                        return False
                    else:
                        print(f"{self.client_id}: Unexpected release response: {response}")
                        if attempt < 2:  # Don't sleep on last attempt
                            time.sleep(1)
                except Exception as e:
                    print(f"{self.client_id}: Error during release attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        time.sleep(1)
            
            # If we get here, all attempts failed
            print(f"{self.client_id}: Failed to release lock after all retries")
            self.lock_acquired = False  # Update local state anyway
            return False
        else:
            print(f"{self.client_id}: Cannot release lock - lock not held")
            return False
    
    def close(self):
        try:
            if self.lock_acquired:
                self.release_lock()
            if self.connection:
                self.connection.close()
            print(f"{self.client_id}: Connection closed")
        except Exception as e:
            print(f"{self.client_id}: Error during close: {e}")

    def send_heartbeat(self):
        while self.lock_acquired:
            response = self.send_message("heartbeat")
            if response != "lease renewed":
                print(f"{self.client_id}: Failed to renew lease, response: {response}")
                self.lock_acquired = False
                self.lease_duration = None  # Reset lease duration on release
                break
            time.sleep(self.lease_duration / 3)


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
            elif response == "File not found":
                print(f"{self.client_id}: Append failed - file not found.")
                return False
            else:
                print(f"{self.client_id}: Append failed or no response - {response}")
                return response
        else:
            print(f"{self.client_id}: Cannot append to file - lock not held.")
            return "lock not held"

    def start_message_listener(self):
        def listen_for_messages():
            while True:
                try:
                    data, _ = self.connection.sock.recvfrom(1024)
                    message = data.decode()
                    if message == "lock expired":
                        print(f"{self.client_id}: Lock expired notification received.")
                        self.lock_acquired = False
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"{self.client_id}: Error in message listener: {e}")
                    break

        listener_thread = threading.Thread(target=listen_for_messages, daemon=True)
        listener_thread.start()

    def close(self):
        if self.connection:
            self.connection.close()
        print(f"{self.client_id}: Connection closed.")

if __name__ == "__main__":
    # List of known server addresses
    servers = [('localhost', 8080), ('localhost', 8082), ('localhost', 8084)]

    # Create two clients
    client1 = DistributedClient("client_1", servers)

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

    # Start the threads
    thread1.start()

    # Wait for both threads to finish
    thread1.join()


    print("All clients have completed their operations.")
