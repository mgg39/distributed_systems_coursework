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
            # Send message to the server
            self.sock.sendto(message.encode(), self.server_address)
            # Receive response from the server
            response, _ = self.sock.recvfrom(1024)
            return response.decode()
        except socket.timeout:
            print(f"{self.client_id}: No response from server (timeout)")
            return ""  # Return an empty string if no response


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
    lock_acquired = False

    # Attempt to acquire lock, retry if necessary
    for _ in range(5):  # Try up to 5 times
        response = client.send_message("acquire_lock")
        if "Lock acquired" in response:
            lock_acquired = True
            print(f"{client_id}: Lock acquired")
            break
        elif "Lock is currently held" in response:
            print(f"{client_id}: {response}")  # "Lock is currently held"
            time.sleep(1)  # Wait briefly before retrying
        else:
            print(f"{client_id}: Retrying due to no response or unknown message")
            time.sleep(1)  # Retry after a short delay

    if lock_acquired:
        # Append data to file if the lock was acquired
        client.append_to_file("file_0", f"Data from {client_id}")
        time.sleep(1)

        # Release the lock
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
