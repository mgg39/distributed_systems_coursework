import socket

class RPCConnection:
    def __init__(self, server_address, server_port):
        self.server_address = server_address
        self.server_port = server_port
        self.sock = None

    def rpc_init(self):
        try:
            # Initialize the socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP socket
            self.sock.settimeout(2)  # Set a timeout for retries

            # Define the server address and port
            server = (self.server_address, self.server_port)

            # Send a simple test message
            message = "Hello, server!"
            print(f"Sending: {message}")
            self.sock.sendto(message.encode(), server)

            # Wait for an acknowledgment from the server
            data, _ = self.sock.recvfrom(1024)  # Buffer size is 1024 bytes
            print(f"Received from server: {data.decode()}")

            return 0  # Return code 0 indicates success

        except socket.timeout:
            print("Timeout: No response from server")
            return 1  # Return code 1 indicates failure

        except Exception as e:
            print(f"An error occurred: {e}")
            return 1

    def close(self):
        if self.sock:
            self.sock.close()
            print("Socket closed")