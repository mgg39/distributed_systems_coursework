import socket
import time

class RPCConnection:
    def __init__(self, server_address, server_port, timeout=2, retries=2, backoff=2):
        # Set reduced timeout and retries for quick feedback during testing
        self.server_address = (server_address, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(timeout)
        self.retries = retries
        self.backoff = backoff

    def rpc_send(self, message):
        attempt = 0
        backoff_time = self.backoff
        while attempt <= self.retries:
            try:
                print(f"Attempt {attempt+1}: Sending '{message}' to {self.server_address}")
                self.sock.sendto(message.encode(), self.server_address)
                data, _ = self.sock.recvfrom(1024)  # Increase recvfrom timeout here if needed
                return data.decode()
            except socket.timeout:
                print(f"Attempt {attempt+1}: Timeout, retrying...")
                attempt += 1
                if attempt > self.retries:
                    return "Timeout: No response after retries"
                time.sleep(self.backoff)
                backoff_time *= 2
            except Exception as e:
                print(f"Attempt {attempt+1}: Error - {e}")
                print(f"Message error: {message}")
                attempt += 1
                if attempt > self.retries:
                    return f"Error after retries: {e}"
                time.sleep(self.backoff)
                backoff_time *= 2

    def close(self):
        if self.sock:
            self.sock.close()
            print("Socket closed")