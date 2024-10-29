import socket

class LockManagerServer:
    def __init__(self, host='localhost', port=8080):
        self.server_address = (host, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(self.server_address)
        self.current_lock_holder = None

    def start(self):
        print(f"Server listening on {self.server_address}")
        while True:
            data, client_address = self.sock.recvfrom(1024)
            message = data.decode()
            print(f"Received message: {message} from {client_address}")
            
            if message == "acquire_lock":
                response = self.acquire_lock(client_address)
            elif message == "release_lock":
                response = self.release_lock(client_address)
            else:
                response = "Unknown command"
            
            self.sock.sendto(response.encode(), client_address)

    def acquire_lock(self, client_id):
        if self.current_lock_holder is None:
            self.current_lock_holder = client_id
            return "Lock acquired"
        else:
            return "Lock is currently held"

    def release_lock(self, client_id):
        if self.current_lock_holder == client_id:
            self.current_lock_holder = None
            return "Lock released"
        else:
            return "You do not hold the lock"

if __name__ == "__main__":
    server = LockManagerServer()
    server.start()
