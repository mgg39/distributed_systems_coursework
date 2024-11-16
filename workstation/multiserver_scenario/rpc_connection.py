import socket
import time

class RPCConnection:
    def __init__(self, host, port):
        self.server_address = (host, port) if host and port else None
        self.sock = None
        if self.server_address:
            self.connect()

    def connect(self):
        try:
            if self.sock:
                self.sock.close()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(2)  # 2 second timeout
        except Exception as e:
            print(f"Error creating socket: {e}")
            raise

    def rpc_send(self, message, max_retries=3):
        if not self.server_address:
            return "Error: No server address"
        
        for attempt in range(max_retries):
            try:
                print(f"Attempt {attempt + 1}: Sending '{message}' to {self.server_address}")
                if not self.sock:
                    self.connect()
                self.sock.sendto(message.encode(), self.server_address)
                response, _ = self.sock.recvfrom(1024)
                return response.decode()
            except socket.timeout:
                print(f"Attempt {attempt + 1}: Timeout, retrying...")
                if attempt == max_retries - 1:
                    return f"Error after retries: No response or invalid socket"
                time.sleep(0.5)  # Short delay before retry
            except Exception as e:
                print(f"Error during RPC: {e}")
                return f"Error: {str(e)}"

    def close(self):
        try:
            if self.sock:
                self.sock.close()
                print("Socket closed")
        except Exception as e:
            print(f"Error closing socket: {e}")