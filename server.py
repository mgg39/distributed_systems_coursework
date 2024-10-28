import socket
import threading
import os

# Define server configurations
SERVER_IP = '127.0.0.1'
SERVER_PORT = 8080
BUFFER_SIZE = 1024
FILES_DIR = "server_files"

# Ensure the directory for files exists
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)

# Create 100 files named "file_0" to "file_99"
for i in range(100):
    file_path = os.path.join(FILES_DIR, f"file_{i}")
    with open(file_path, 'a') as f:  # 'a' mode creates the file if it doesn't exist
        pass

# A global lock to control access to files
file_lock = threading.Lock()

# Function to handle each client request
def handle_client(client_socket, client_address):
    print(f"Connected to client at {client_address}")

    try:
        # Receive a message from the client
        message, _ = client_socket.recvfrom(BUFFER_SIZE)
        decoded_message = message.decode()

        # Respond to initialization check
        if decoded_message == "Hello, server!":
            response = "Hello, client!"
            client_socket.sendto(response.encode(), client_address)
            print("Sent acknowledgment to client")
        else:
            print("Received unknown message:", decoded_message)

        # Example logic for lock acquire and release (not implemented yet)
        # You can add more cases based on client requests

    except Exception as e:
        print(f"Error with client {client_address}: {e}")
    
    finally:
        client_socket.close()
        print(f"Connection closed with {client_address}")

# Main server loop
def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((SERVER_IP, SERVER_PORT))
    print(f"Server listening on {SERVER_IP}:{SERVER_PORT}")

    while True:
        try:
            # Accept incoming client connections
            data, client_address = server_socket.recvfrom(BUFFER_SIZE)
            print(f"Received data from {client_address}")

            # Spawn a new thread to handle each client
            client_thread = threading.Thread(target=handle_client, args=(server_socket, client_address))
            client_thread.start()

        except KeyboardInterrupt:
            print("Server is shutting down.")
            break

    server_socket.close()

if __name__ == "__main__":
    start_server()
