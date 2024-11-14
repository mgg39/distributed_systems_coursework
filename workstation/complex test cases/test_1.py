import socket
import time

def send_request(server_host, server_port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(5)  # Increase to 5 seconds for debugging
        sock.sendto(message.encode(), (server_host, server_port))
        try:
            response, _ = sock.recvfrom(1024)
            return response.decode()
        except socket.timeout:
            return "[ERROR] No response from server."

# Test functions
def acquire_lock_test():
    response = send_request("localhost", 8080, "acquire_lock:client_1")
    print("[TEST] Acquire Lock:", response)

def append_test(data):
    for file_id in range(1, 4):
        message = f"append_file:client_1:file_{file_id}:{data}"
        response = send_request("localhost", 8080, message)
        print(f"[TEST] Append to file_{file_id}:", response)

def release_lock_test():
    response = send_request("localhost", 8080, "release_lock:client_1")
    print("[TEST] Release Lock:", response)

# Example usage
acquire_lock_test()
append_test("A")
release_lock_test()
