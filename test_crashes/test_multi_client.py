import threading
from dc_simulated_crashes import DistributedClient
import time

def client_actions(client_id):
    # Map client IDs to specific files
    file_map = {
        "client_1": "file_0",
        "client_3": "file_0",
        "client_2": "file_2",
        "client_4": "file_3",
        "client_5": "file_3"
    }
    
    # Determine the file to which this client should write
    file_name = file_map.get(client_id, "file_0")  # Default to file_0 if client ID is not mapped
    client = DistributedClient(client_id)
    
    try:
        # Try to acquire lock with retries
        if client.acquire_lock():
            # Append data to file if lock acquired
            client.append_file(file_name, f"Log entry from {client_id} writing to {file_name}")
            print(f"{client_id} successfully wrote to {file_name}")
        else:
            print(f"{client_id} could not acquire lock and did not write.")
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
