import time
import threading
from distributed_client import DistributedClient

def client_1_operations():
    client = DistributedClient("client_1", [('localhost', 8080), ('localhost', 8082), ('localhost', 8084)])
    if client.acquire_lock():
        # Append 'A' to file_1, file_2, and file_3
        for file_name in ["file_1", "file_2", "file_3"]:
            client.append_file(file_name, "A")
            time.sleep(1)  # Small delay between appends to simulate real-time actions
        client.release_lock()
    client.close()

def client_2_operations():
    client = DistributedClient("client_2", [('localhost', 8080), ('localhost', 8082), ('localhost', 8084)])
    # Wait a bit before trying to acquire the lock to simulate contention
    time.sleep(2)  
    if client.acquire_lock():
        # Append 'B' to file_1, file_2, and file_3
        for file_name in ["file_1", "file_2", "file_3"]:
            client.append_file(file_name, "B")
            time.sleep(1)
        client.release_lock()
    client.close()

def simulate_server_failure_and_recovery(server_process):
    # Simulate failure of Server 2 during Client 1's operation
    time.sleep(3)  # Wait for Client 1 to start appending
    print("[TEST] Simulating Server 2 failure...")
    server_process.terminate()  # This assumes server_process is a subprocess
    server_process.wait()  # Ensure the server process is stopped

    # Simulate quick recovery of Server 2
    time.sleep(2)  # Simulate downtime duration
    print("[TEST] Restarting Server 2 for recovery...")
    # Assuming we have a function `start_server` to restart Server 2
    server_process = start_server(port=8082, server_id=2)  # Replace with actual restart function
    return server_process

def start_server(port, server_id):
    import subprocess
    return subprocess.Popen(["python", "lock_manager_server.py", str(port), str(server_id)])

def run_test_case():
    # Start servers
    server1_process = start_server(8080, 1)  # Leader
    server2_process = start_server(8082, 2)  # Replica
    server3_process = start_server(8084, 3)  # Replica

    try:
        # Run clients in parallel
        client1_thread = threading.Thread(target=client_1_operations)
        client2_thread = threading.Thread(target=client_2_operations)

        # Start Client 1 and Client 2
        client1_thread.start()
        time.sleep(1)  # Ensure Client 1 starts first
        client2_thread.start()

        # Simulate Server 2 failure and recovery
        server2_process = simulate_server_failure_and_recovery(server2_process)

        # Wait for both clients to complete their operations
        client1_thread.join()
        client2_thread.join()

    finally:
        # Clean up and terminate servers
        server1_process.terminate()
        server1_process.wait()
        server2_process.terminate()
        server2_process.wait()
        server3_process.terminate()
        server3_process.wait()
        print("[TEST] Test case completed. Check the files on each server for consistency.")

if __name__ == "__main__":
    run_test_case()
