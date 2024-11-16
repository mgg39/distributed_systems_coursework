
import socket
import time

class RPCConnection:
    def __init__(self, host, port):
        self.server_address = (host, port) if host and port else None
        self.sock = None
        if self.server_address:
            print(f"[DEBUG] Initializing connection to {self.server_address}")
            self.connect()

    def connect(self):
        try:
            print(f"[DEBUG] Connecting to {self.server_address}")
            if self.sock:
                self.sock.close()
                print("[DEBUG] Previous socket closed before reinitializing.")
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(2)  # 2 second timeout
        except Exception as e:
            print(f"[ERROR] Error creating socket: {e}")
            raise RuntimeError("Failed to create or initialize socket") from e

    def rpc_send(self, message, max_retries=3):
        if not self.server_address:
            return "Error: No server address"

        for attempt in range(max_retries):
            try:
                print(f"[DEBUG] Attempt {attempt + 1}: Sending '{message}' to {self.server_address}")
                if not self.sock:
                    print("[DEBUG] Socket is missing. Reinitializing.")
                    self.connect()
                self.sock.sendto(message.encode(), self.server_address)
                response, _ = self.sock.recvfrom(1024)
                print(f"[DEBUG] Received response: {response.decode()}")
                return response.decode()
            except socket.timeout:
                print(f"[DEBUG] Attempt {attempt + 1}: Timeout, retrying...")
                if attempt == max_retries - 1:
                    return "Error after retries: No response or invalid socket"
                time.sleep(0.5)  # Short delay before retry
            except OSError as e:
                print(f"[ERROR] Network error during RPC: {e}")
                self.connect()  # Attempt to reinitialize the connection
            except Exception as e:
                print(f"[ERROR] Unexpected error during RPC: {e}")
                return f"Error: {str(e)}"

    def close(self):
        try:
            if self.sock:
                self.sock.close()
                print("[DEBUG] Socket closed successfully.")
        except Exception as e:
            print(f"[ERROR] Error closing socket: {e}")
