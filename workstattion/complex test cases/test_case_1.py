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

import time
from distributed_client import DistributedClient
from distributed_server import DistributedServer

def test_replica_node_failures_fast_recovery():
    test_id = "replica_node_failures_fast_recovery"
    consistency_model = "strong"  # Or "weak"

    try:
        # Setup Clients and Servers
        server_1 = DistributedServer(server_id="server_1", is_primary=True)
        server_2 = DistributedServer(server_id="server_2", is_primary=False)
        server_3 = DistributedServer(server_id="server_3", is_primary=False)
        
        client_1 = DistributedClient(client_id="client_1", replicas=[server_1, server_2, server_3])
        client_2 = DistributedClient(client_id="client_2", replicas=[server_1, server_2, server_3])

        # Client 1 acquires lock
        log_to_file_and_console(f"{test_id} - Client 1 acquiring lock", f)
        lock_acquired_1 = client_1.acquire_lock()
        assert lock_acquired_1, "Client 1 failed to acquire lock"
        
        # Client 1 appends 'A' to files
        client_1.append_file("file_1", "A")
        client_1.append_file("file_2", "A")
        client_1.append_file("file_3", "A")
        
        # Server 2 crashes after the append
        log_to_file_and_console(f"{test_id} - Server 2 crashing", f)
        server_2.simulate_crash()

        # Client 2 attempts to acquire lock
        log_to_file_and_console(f"{test_id} - Client 2 acquiring lock", f)
        lock_acquired_2 = client_2.acquire_lock()
        assert lock_acquired_2, "Client 2 failed to acquire lock"

        # Client 2 appends 'B' to files
        client_2.append_file("file_1", "B")
        client_2.append_file("file_2", "B")
        client_2.append_file("file_3", "B")

        # Server 2 recovers quickly
        log_to_file_and_console(f"{test_id} - Server 2 recovering", f)
        server_2.recover()

        # Synchronize data with Server 1 and Server 2
        server_1.sync_with_replicas([server_2, server_3])
        
        # Assert that file contents are consistent
        assert check_file_contents("file_1", "A B"), "Inconsistent file_1"
        assert check_file_contents("file_2", "A B"), "Inconsistent file_2"
        assert check_file_contents("file_3", "A B"), "Inconsistent file_3"

        # Test Passes if no assertions fail
        log_result(test_id, True)

    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result(test_id, False)

# Helper functions for logging and checking file contents
def log_to_file_and_console(message, f):
    print(message)
    f.write(message + "\n")

def log_result(test_id, result):
    print(f"Test {test_id} result: {'PASS' if result else 'FAIL'}")

