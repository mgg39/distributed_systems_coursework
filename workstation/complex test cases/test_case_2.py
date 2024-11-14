# Primary Node Failures (Slow Recovery during Critical Sections)
"""
Scenario Overview:
-------------------
Client 1, Client 2, and Client 3 each want to append 'A', 'B', and 'C' to a file.
After Client 1 completes its operations, Server 1 (the primary node) crashes during Client 2's operations.
Server 2 and Server 3 (replicas) elect a new primary node.
Server 1 recovers after the new primary is elected and synchronizes with the new primary to ensure consistency.

Steps:
-------
Client 1 appends 'A' to file_1 5 times, and Server 1 successfully commits these changes.
Client 2 begins appending 'B' to file_1, but Server 1 crashes before completing all operations.
Server 2 or Server 3 becomes the new primary, and Client 2 sends its remaining operations to the new primary.
Client 3 appends 'C' after Client 2's operations.
Server 1 recovers and synchronizes with the new primary, ensuring consistency.

Expected Results:
------------------
-> Atomicity:
The system guarantees that each client’s operations are atomic, so Client 1’s appends ('A') are sequentially written, followed by Client 2’s appends ('B'), and Client 3’s appends ('C').
The final result for file_1 could either be "AAAAABBBBBCCCCC" or "BBBBBAAAAACCCCC", depending on whether the lock was handed over to Client 2 before Server 1’s crash.

-> For Strong Consistency:
Once Server 1 recovers, it synchronizes with the new primary to ensure the consistency of all file changes.
The final files should be consistent and reflect the order of operations (either "A B C" or "B A C").

-> For Weak/Bounded Consistency:
The system allows some temporary inconsistency between the servers while Server 1 is down.
The final file contents may show some stale data until Server 1 synchronizes with the new primary, but eventually, consistency will be achieved.
"""

def test_primary_node_failures_slow_recovery():
    test_id = "primary_node_failures_slow_recovery"
    consistency_model = "strong"  # Or "weak"

    try:
        # Setup Clients and Servers
        server_1 = DistributedServer(server_id="server_1", is_primary=True)
        server_2 = DistributedServer(server_id="server_2", is_primary=False)
        server_3 = DistributedServer(server_id="server_3", is_primary=False)
        
        client_1 = DistributedClient(client_id="client_1", replicas=[server_1, server_2, server_3])
        client_2 = DistributedClient(client_id="client_2", replicas=[server_1, server_2, server_3])
        client_3 = DistributedClient(client_id="client_3", replicas=[server_1, server_2, server_3])

        # Client 1 appends 'A' to files
        client_1.append_file("file_1", "A")
        client_1.append_file("file_2", "A")
        client_1.append_file("file_3", "A")

        # Server 1 (Primary) crashes during Client 2's operation
        server_1.simulate_crash()

        # New primary is selected (Server 2 or Server 3)
        new_primary = server_2 if server_3.is_primary else server_3
        log_to_file_and_console(f"{test_id} - New primary selected: {new_primary.server_id}", f)
        
        # Client 2 and Client 3 continue their operations
        client_2.append_file("file_1", "B")
        client_2.append_file("file_2", "B")
        client_2.append_file("file_3", "B")
        
        client_3.append_file("file_1", "C")
        client_3.append_file("file_2", "C")
        client_3.append_file("file_3", "C")

        # Ensure synchronization after recovery
        server_1.recover()
        server_1.sync_with_replicas([server_2, server_3])

        # Assert final file contents
        assert check_file_contents("file_1", "A B C"), "Inconsistent file_1"
        assert check_file_contents("file_2", "A B C"), "Inconsistent file_2"
        assert check_file_contents("file_3", "A B C"), "Inconsistent file_3"

        log_result(test_id, True)

    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result(test_id, False)
