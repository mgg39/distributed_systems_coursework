import socket
import time
import threading

class TestClient:
    def __init__(self, client_id, server_address='localhost', server_port=8080):
        self.client_id = client_id
        self.server_address = (server_address, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send_message(self, message):
        try:
            # Send message 
            self.sock.sendto(message.encode(), self.server_address)
            # Receive response 
            response, _ = self.sock.recvfrom(1024)
            print(f"{self.client_id}: {response.decode()}")
        except socket.timeout:
            print(f"{self.client_id}: No response from server")

    def acquire_lock(self):
        self.send_message("acquire_lock")

    def release_lock(self):
        self.send_message("release_lock")

    def append_to_file(self, file_name, data):
        self.send_message(f"append_file:{file_name}:{data}")

    def close(self):
        self.sock.close()

# Test with multiple clients
def client_actions(client_id):
    client = TestClient(client_id)
    
    # Step 1: Attempt to acquire lock
    client.acquire_lock()
    time.sleep(1)
    
    # Step 2: Append data to file (while holding lock)
    client.append_to_file("file_0", f"Data from {client_id}")
    time.sleep(1)

    # Step 3: Release the lock
    client.release_lock()
    
    client.close()

if __name__ == "__main__":
    # Simulate multiple clients in separate threads
    clients = ["client_1", "client_2", "client_3"]
    threads = []

    for client_id in clients:
        thread = threading.Thread(target=client_actions, args=(client_id,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()
