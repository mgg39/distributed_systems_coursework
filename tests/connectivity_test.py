import socket
import time

def test_connectivity():
    server_address = ("localhost", 8080)  # Adjust if using another port
    max_retries = 5  # Set number of retries
    for attempt in range(max_retries):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(2)  # Increase the timeout slightly
            try:
                print(f"Attempt {attempt + 1}: Sending ping to server...")
                sock.sendto("ping".encode(), server_address)
                response, _ = sock.recvfrom(1024)
                if response.decode() == "pong":
                    print("Server is responsive!")
                    return
                else:
                    print("Unexpected response:", response.decode())
            except socket.timeout:
                print(f"Attempt {attempt + 1}: No response, retrying...")
                time.sleep(1)  # Brief pause before retry
    print("No response from server; check if it’s running on the specified port.")

if __name__ == "__main__":
    test_connectivity()
