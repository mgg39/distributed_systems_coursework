import socket

class RPCConnection:
    def __init__(self, server_address, server_port):
        self.server_address = (server_address, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(2)

    def rpc_send(self, message):
        try:
            # Send message
            print(f"Sending: {message} to {self.server_address}")
            self.sock.sendto(message.encode(), self.server_address)

            # Receive response
            data, _ = self.sock.recvfrom(1024)
            return data.decode()

        except socket.timeout:
            return "Timeout: No response from server"

        except Exception as e:
            return f"Error: {e}"

    def close(self):
        if self.sock:
            self.sock.close()
            print("Socket closed")
