import socket
import time

class RPCConnection:
    def __init__(self, server_address, server_port, timeout=5, retries=3, backoff=2):
        self.server_address = (server_address, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(timeout)
        self.retries = retries  # Max retries
        self.backoff = backoff  # Wait time btw retries

def rpc_send(self, message):
    attempt = 0
    while attempt <= self.retries:
        try:
            self.sock.sendto(message.encode(), self.server_address)
            data, _ = self.sock.recvfrom(1024)
            return data.decode()
        except socket.timeout:
            attempt += 1
            if attempt > self.retries:
                return "Timeout: No response after retries"
            time.sleep(self.backoff)
        except Exception as e:
            attempt += 1
            if attempt > self.retries:
                return f"Error after retries: {e}"
            time.sleep(self.backoff)

    def close(self):
        if self.sock:
            self.sock.close()
            print("Socket closed")
