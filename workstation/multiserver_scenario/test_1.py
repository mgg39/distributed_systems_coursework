import socket
import time

SERVER_ADDRESS = 'localhost'

def send_request(message, port):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(message.encode(), (SERVER_ADDRESS, port))
        response, _ = sock.recvfrom(1024)
        print(f"Response: {response.decode()}")

# Simulate a client trying to acquire a lock
def acquire_lock(client_id):
    message = f"acquire_lock:{client_id}"
    send_request(message, 8080)  # Send to the leader's address

# Simulate a client trying to release a lock
def release_lock(client_id):
    message = f"release_lock:{client_id}"
    send_request(message, 8080)  # Send to the leader's address

# Simulate a client appending data to a file
def append_to_file(client_id, file_name, data):
    message = f"append_file:{client_id}:{file_name}:{data}"
    send_request(message, 8080)  # Send to the leader's address

if __name__ == "__main__":
    client_id = "client_1"
    acquire_lock(client_id)  # Client 1 tries to acquire the lock
    time.sleep(2)
    append_to_file(client_id, "file_0", "Sample data")
    time.sleep(2)
    release_lock(client_id)  # Client 1 releases the lock
