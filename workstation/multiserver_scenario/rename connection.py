import socket

def ping_server(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(1)
        try:
            sock.sendto(b"ping", (host, port))
            response, _ = sock.recvfrom(1024)
            print(f"Response from {host}:{port} - {response.decode()}")
        except socket.timeout:
            print(f"No response from {host}:{port}")

ping_server('localhost', 8080)  # Test each server port individually
ping_server('localhost', 8082)
ping_server('localhost', 8084)
