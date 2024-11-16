import socket
import threading
from queue import Queue

class LockManagerServer:
    def __init__(self, server_address):
        self.server_address = server_address
        self.lock_holder = None  # Tracks the current lock holder
        self.queue = Queue()  # Queue for waiting clients
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(server_address)
        self.running = True

    def start(self):
        print(f"[DEBUG] Server listening on {self.server_address}")
        threading.Thread(target=self.listen, daemon=True).start()

    def listen(self):
        while self.running:
            try:
                data, client_address = self.socket.recvfrom(1024)
                message = data.decode()
                print(f"[DEBUG] Received message: {message} from {client_address}")
                threading.Thread(target=self.handle_request, args=(message, client_address)).start()
            except Exception as e:
                print(f"[ERROR] Listening error: {e}")

    def handle_request(self, message, client_address):
        try:
            command, *args = message.split(":")
            if command == "acquire_lock":
                self.handle_acquire_lock(client_address)
            elif command == "release_lock":
                self.handle_release_lock(client_address)
            else:
                self.send_response("Unknown command", client_address)
        except Exception as e:
            print(f"[ERROR] Request handling error: {e}")

    def handle_acquire_lock(self, client_address):
        if not self.lock_holder:
            self.lock_holder = client_address
            self.send_response("grant lock:20", client_address)
            print(f"[DEBUG] Lock granted to {client_address}. Current queue: {list(self.queue.queue)}")
        else:
            self.queue.put(client_address)
            position = self.queue.qsize()
            self.send_response(f"queued:position_{position}", client_address)
            print(f"[DEBUG] Client {client_address} added to queue at position {position}")

    def handle_release_lock(self, client_address):
        if self.lock_holder == client_address:
            print(f"[DEBUG] Lock released by {client_address}. Queue before processing: {list(self.queue.queue)}")
            self.lock_holder = None
            self.process_queue()
        else:
            self.send_response("You do not hold the lock", client_address)
            print(f"[DEBUG] Release lock failed. {client_address} does not hold the lock.")

    def process_queue(self):
        try:
            print(f"[DEBUG] Starting process_queue. Current queue: {list(self.queue.queue)}, Current holder: {self.lock_holder}")
            if not self.lock_holder and not self.queue.empty():
                next_client = self.queue.get()
                self.lock_holder = next_client
                self.send_response("grant lock:20", next_client)
                print(f"[DEBUG] Lock granted to {next_client}. Queue after processing: {list(self.queue.queue)}")
            else:
                print(f"[DEBUG] Skipping process_queue - Queue empty or lock already held. Current holder: {self.lock_holder}")
        except Exception as e:
            print(f"[ERROR] Queue processing error: {e}")

    def send_response(self, response, client_address):
        try:
            self.socket.sendto(response.encode(), client_address)
            print(f"[DEBUG] Sent response: {response} to {client_address}")
        except Exception as e:
            print(f"[ERROR] Error sending response to {client_address}: {e}")

    def shutdown(self):
        self.running = False
        self.socket.close()
        print("[DEBUG] Server shutdown.")

if __name__ == "__main__":
    server = LockManagerServer(("localhost", 8080))
    try:
        server.start()
        while True:
            pass  # Keep server running
    except KeyboardInterrupt:
        server.shutdown()
