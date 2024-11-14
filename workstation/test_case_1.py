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
import subprocess
import threading
import time
from distributed_client import DistributedClient
from upd_server import LockManagerServer
import os

"""
def start_server(server_id, port, peers):
    env = os.environ.copy()
    env["PYTHONPYCACHEPREFIX"] = f"./server_working_dir_{server_id}/__pycache__"
    return subprocess.Popen(
        ["python", "server_code.py", "--server_id", str(server_id), "--port", str(port)],
        env=env
    )
"""
import subprocess
import threading
import time
from distributed_client import DistributedClient
from upd_server import LockManagerServer
import os

def start_server(server_id, port, peers):
    """
    Start each server as a subprocess in its own isolated working directory.
    """
    # Define the working directory for the server in `workstation`
    working_dir = os.path.join("distributed_systems", f"server_{server_id}")

    # Create the directory if it doesn't exist
    os.makedirs(working_dir, exist_ok=True)

    # Start the server as a subprocess in its own directory
    return subprocess.Popen(
        ["python3", "upd_server.py", "--server_id", str(server_id), "--port", str(port)],
        cwd=working_dir,  # Set the working directory for isolation
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

def test_replica_node_failures_fast_recovery():
    test_id = "replica_node_failures_fast_recovery"
    try:
        # Start servers with isolated directories for their files
        server_1 = start_server(server_id=1, port=8080, peers=[("localhost", 8082), ("localhost", 8084)])
        server_2 = start_server(server_id=2, port=8082, peers=[("localhost", 8080), ("localhost", 8084)])
        server_3 = start_server(server_id=3, port=8084, peers=[("localhost", 8080), ("localhost", 8082)])

        # Give time for the servers to initialize and elect a leader
        time.sleep(5)

        # Create clients
        client_1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080), ("localhost", 8082), ("localhost", 8084)])
        client_2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080), ("localhost", 8082), ("localhost", 8084)])

        # Wait for the leader to be elected
        time.sleep(10)

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

        # Simulate crash of server 2
        print(f"{test_id}: Lock Server 2 crashing")
        server_2.terminate()  # Simulate crash by terminating the process

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

        # Restart Server 2 to simulate recovery
        print(f"{test_id}: Lock Server 2 recovering")
        server_2 = start_server(server_id=2, port=8082, peers=[("localhost", 8080), ("localhost", 8084)])
        
        # Synchronize data with Lock Server 1 and Lock Server 2
        print(f"{test_id}: Synchronizing data with Lock Server 2")
        time.sleep(5)  # Give some time for synchronization to occur

        # Check and print file contents for consistency
        print(f"{test_id}: Checking file contents for consistency")
        assert check_file_contents("file_1", "A\nB", server_id=1), f"{test_id}: Inconsistent file_1"
        print(f"{test_id}: file_1 is consistent")

        assert check_file_contents("file_2", "A\nB", server_id=1), f"{test_id}: Inconsistent file_2"
        print(f"{test_id}: file_2 is consistent")

        assert check_file_contents("file_3", "A\nB", server_id=1), f"{test_id}: Inconsistent file_3"
        print(f"{test_id}: file_3 is consistent")

        print(f"{test_id}: Test passed")

    except Exception as e:
        print(f"{test_id}: Test failed - {e}")

    finally:
        # Terminate all server processes
        server_1.terminate()
        server_2.terminate()
        server_3.terminate()

def check_file_contents(file_name, expected_contents, server_id):
    file_path = os.path.join(f"server_files_{server_id}", file_name)
    try:
        with open(file_path, 'r') as file:
            file_contents = file.read().strip()
            return file_contents == expected_contents
    except Exception as e:
        print(f"Error reading file {file_name} on server {server_id}: {e}")
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