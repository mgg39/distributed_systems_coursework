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
    
    # Client 1 tries to acquire the lock
    acquire_lock(client_id)  
    time.sleep(2)
    
    # Client 1 appends data to file and waits for leader acknowledgment
    append_to_file(client_id, "file_0", "Sample data")
    
    # Allow time for file update to be processed and acknowledgment received
    time.sleep(2)
    
    # Client 1 releases the lock
    release_lock(client_id)  
