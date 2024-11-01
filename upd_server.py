import socket
import os
import threading

# Configuration
FILES_DIR = "server_files"
NUM_FILES = 100

# Ensure the files directory exists and create 100 files
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)
for i in range(NUM_FILES):
    open(os.path.join(FILES_DIR, f"file_{i}"), 'a').close()

class LockManagerServer:
    def __init__(self, host='localhost', port=8080):
        self.server_address = (host, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(self.server_address)
        self.lock = threading.Lock()
        self.current_lock_holder = None

    def start(self):
        print(f"Server listening on {self.server_address}")
        while True:
            data, client_address = self.sock.recvfrom(1024)
            print(f"Received message from {client_address}")
            threading.Thread(target=self.handle_request, args=(data, client_address)).start()

    def handle_request(self, data, client_address):
        message = data.decode()
        print(f"Handling message: {message} from {client_address}")

        # Process commands
        if message == "acquire_lock":
            response = self.acquire_lock(client_address)
        elif message == "release_lock":
            response = self.release_lock(client_address)
        elif message.startswith("append_file"):
            _, file_name, file_data = message.split(":", 2)
            response = self.append_to_file(client_address, file_name, file_data)
        else:
            response = "Unknown command"

        # Send response back to the client
        self.sock.sendto(response.encode(), client_address)

    def acquire_lock(self, client_id):
        with self.lock:
            if self.current_lock_holder is None:
                self.current_lock_holder = client_id
                print(f"Lock granted to {client_id}")
                return "grant lock"
            else:
                print(f"Lock is currently held by {self.current_lock_holder}")
                return "Lock is currently held"

    def release_lock(self, client_id):
        with self.lock:
            if self.current_lock_holder == client_id:
                self.current_lock_holder = None
                return "unlock success"
            else:
                return "You do not hold the lock"

    def append_to_file(self, client_id, file_name, data):
        with self.lock:
            if self.current_lock_holder == client_id:
                file_path = os.path.join(FILES_DIR, file_name)
                if os.path.exists(file_path):
                    with open(file_path, 'a') as f:
                        f.write(data + "\n")
                    return f"Data appended to {file_name}"
                else:
                    return f"File {file_name} not found"
            else:
                return "You do not hold the lock"

if __name__ == "__main__":
    server = LockManagerServer()
    server.start()
