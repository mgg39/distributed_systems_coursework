# Replica Node Failures (Fast Recovery)

"""
Scenario Overview:
-------------------
Server 1 is the primary node, and Server 2 and Server 3 are replica nodes.
Client 1 and Client 2 want to append 'A' and 'B' to three files, respectively.
After a series of operations, Server 2 experiences a transient failure but recovers quickly.
After recovery, the system should synchronize correctly depending on the consistency model (e.g., strong consistency vs. weak consistency).

Steps:
-------
Client 1 acquires the lock and appends 'A' to file_1, file_2, and file_3.
Client 2 attempts to acquire the lock and appends 'B' to the same files.
Server 2 crashes during the second file append but recovers quickly.
Server 1 (primary) proceeds with Client 1’s file append, syncing with Server 3 (available replica).
After Server 2 recovers, it synchronizes with Server 1, ensuring consistency of the file and lock states.

Expected Results:
------------------
-> Atomicity: The lock mechanism ensures Client 1’s operations (file_1, file_2, file_3 with 'A') and Client 2’s operations (file_1, file_2, file_3 with 'B') are atomic, with no interference.

-> For Strong Consistency:
Server 1 ensures all changes are propagated to Server 2 upon recovery.
The final contents of the files should either be "A B" or "B A", depending on whether the lock was successfully transferred to Client 2 after Client 1's operations.
Server 1 guarantees that Client 1's writes are completed before Client 2's writes.

-> For Weak Consistency:
Server 1 may proceed without waiting for Server 2 to recover and sync.
After Server 2 recovers, it will synchronize with Server 1 to ensure consistency, but there might be some delay in synchronizing changes between the servers.
Files might temporarily show mixed data, but after recovery, the system will eventually converge to a consistent state.
"""
import threading
import time
from distributed_client import DistributedClient
from upd_server import LockManagerServer

import threading
import time
from distributed_client import DistributedClient
from upd_server import LockManagerServer

import threading
import time
from distributed_client import DistributedClient
from upd_server import LockManagerServer

def start_server(server_instance):
    try:
        print(f"Starting server {server_instance.server_id} on {server_instance.server_address}")
        server_instance.start()
    except Exception as e:
        print(f"Error starting server {server_instance.server_id}: {e}")

def test_replica_node_failures_fast_recovery():
    test_id = "replica_node_failures_fast_recovery"

    try:
        # Setup Clients and Lock Servers
        lock_server_1 = LockManagerServer(server_id=1, host='localhost', port=8080, peers=[('localhost', 8082), ('localhost', 8084)])
        lock_server_2 = LockManagerServer(server_id=2, host='localhost', port=8082, peers=[('localhost', 8080), ('localhost', 8084)])
        lock_server_3 = LockManagerServer(server_id=3, host='localhost', port=8084, peers=[('localhost', 8080), ('localhost', 8082)])

        # Start the servers in separate threads
        thread_1 = threading.Thread(target=start_server, args=(lock_server_1,))
        thread_2 = threading.Thread(target=start_server, args=(lock_server_2,))
        thread_3 = threading.Thread(target=start_server, args=(lock_server_3,))

        # Start the server threads
        thread_1.start()
        time.sleep(1) 
        thread_2.start()
        time.sleep(1) 
        thread_3.start()

        time.sleep(2)  # all threads working

        # Create clients
        client_1 = DistributedClient(client_id="client_1", replicas=[('localhost', 8080), ('localhost', 8082), ('localhost', 8084)])
        client_2 = DistributedClient(client_id="client_2", replicas=[('localhost', 8080), ('localhost', 8082), ('localhost', 8084)])

        time.sleep(10)  # first election delay

        """
        # Wait until a leader is elected before proceeding
        if not client_1.find_leader(retries=10, delay=3):  # Retry for up to 30 seconds in total
            print(f"{test_id}: Failed to identify a leader, aborting test.")
            return

        if not client_2.find_leader(retries=10, delay=3):  # Retry for up to 30 seconds in total
            print(f"{test_id}: Failed to identify a leader, aborting test.")
            return
            """
        # Client 1 acquires lock
        print(f"{test_id}: Client 1 attempting to acquire lock")
        lock_acquired_1 = client_1.acquire_lock()
        if lock_acquired_1:
            print(f"{test_id}: Client 1 successfully acquired lock")
        else:
            print(f"{test_id}: Client 1 failed to acquire lock")
            return
        
        # Client 1 appends 'A' to files
        print(f"{test_id}: Client 1 appending data to files")
        client_1.append_file("file_1", "A")
        client_1.append_file("file_2", "A")
        client_1.append_file("file_3", "A")

        # Client 1 releases lock
        client_1.release_lock()
        print(f"{test_id}: Client 1 released lock")

        # Lock server 2 crashes
        print(f"{test_id}: Lock Server 2 crashing")
        lock_server_2.simulate_crash()

        # Client 2 attempts to acquire lock
        print(f"{test_id}: Client 2 attempting to acquire lock")
        lock_acquired_2 = client_2.acquire_lock()
        if lock_acquired_2:
            print(f"{test_id}: Client 2 successfully acquired lock")
        else:
            print(f"{test_id}: Client 2 failed to acquire lock")
            return

        # Client 2 appends 'B' to files
        print(f"{test_id}: Client 2 appending data to files")
        client_2.append_file("file_1", "B")
        client_2.append_file("file_2", "B")
        client_2.append_file("file_3", "B")

        # Client 2 releases lock
        client_2.release_lock()
        print(f"{test_id}: Client 2 released lock")

        # Lock Server 2 recovers quickly
        print(f"{test_id}: Lock Server 2 recovering")
        lock_server_2.recover(lock_server_1)

        # Synchronize data with Lock Server 1 and Lock Server 2
        print(f"{test_id}: Synchronizing data with Lock Server 2")
        lock_server_1.sync_with_replicas([lock_server_2, lock_server_3])

        # Check and print file contents
        print(f"{test_id}: Checking file contents for consistency")
        assert check_file_contents("file_1", "A\nB"), f"{test_id}: Inconsistent file_1"
        print(f"{test_id}: file_1 is consistent")

        assert check_file_contents("file_2", "A\nB"), f"{test_id}: Inconsistent file_2"
        print(f"{test_id}: file_2 is consistent")

        assert check_file_contents("file_3", "A\nB"), f"{test_id}: Inconsistent file_3"
        print(f"{test_id}: file_3 is consistent")

        # Test Passes if no assertions fail
        print(f"{test_id}: Test passed")

    except Exception as e:
        print(f"{test_id}: Test failed - {e}")

# checking file contents
def check_file_contents(file_name, expected_contents):
    file_path = os.path.join("server_files", file_name)
    try:
        with open(file_path, 'r') as file:
            file_contents = file.read().strip()
            return file_contents == expected_contents
    except Exception as e:
        print(f"Error reading file {file_name}: {e}")
        return False

def main():
    test_replica_node_failures_fast_recovery()

if __name__ == "__main__":
    main()


"""
Expected output
------------------
replica_node_failures_fast_recovery: Client 1 attempting to acquire lock
replica_node_failures_fast_recovery: Client 1 successfully acquired lock
replica_node_failures_fast_recovery: Client 1 appending data to files
replica_node_failures_fast_recovery: Lock Server 2 crashing
replica_node_failures_fast_recovery: Client 2 attempting to acquire lock
replica_node_failures_fast_recovery: Client 2 successfully acquired lock
replica_node_failures_fast_recovery: Client 2 appending data to files
replica_node_failures_fast_recovery: Lock Server 2 recovering
replica_node_failures_fast_recovery: Synchronizing data with Lock Server 2
replica_node_failures_fast_recovery: Checking file contents for consistency
replica_node_failures_fast_recovery: file_1 is consistent
replica_node_failures_fast_recovery: file_2 is consistent
replica_node_failures_fast_recovery: file_3 is consistent
replica_node_failures_fast_recovery: Test passed

"""