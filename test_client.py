import threading
from distributed_client import DistributedClient
import time

def client_actions(client_id):
    client = DistributedClient(client_id)
    
    try:
        # Try to acquire lock with retries
        if client.acquire_lock():
            # Append data to file if lock acquired
            client.append_file("file_0", f"Log entry from {client_id}")
    finally:
        # Ensure lock is released after operations are complete
        client.release_lock()
        # Close the connection
        client.close()

if __name__ == "__main__":
    # List of client IDs for testing
    client_ids = [f"client_{i}" for i in range(1, 6)]
    threads = []

    # Start each client in a separate thread
    for client_id in client_ids:
        thread = threading.Thread(target=client_actions, args=(client_id,))
        threads.append(thread)
        thread.start()
        time.sleep(0.5)  # Optional: slight delay to stagger client starts

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("All clients have completed their operations.")
