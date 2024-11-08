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
        attempt = 0  # Track attempts
        
        while attempt <= self.retries:
            try:
                # Attempt to send msg
                print(f"Attempt {attempt+1}: Sending '{message}' to {self.server_address}")
                self.sock.sendto(message.encode(), self.server_address)

                # Try to get response
                data, _ = self.sock.recvfrom(1024)
                return data.decode()

            except socket.timeout:
                print(f"Attempt {attempt+1}: Timeout, retrying...")
                attempt += 1
                if attempt > self.retries:
                    return "Timeout: No response after retries"
                time.sleep(self.backoff)  # Wait, then retry

            except Exception as e:
                print(f"Attempt {attempt+1}: Error - {e}")
                attempt += 1
                if attempt > self.retries:
                    return f"Error after retries: {e}"
                time.sleep(self.backoff)  # Wait, then retry

    def close(self):
        if self.sock:
            self.sock.close()
            print("Socket closed")
