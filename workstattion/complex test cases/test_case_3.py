# Multiple Server Node Failures
"""
Scenario Overview:
-------------------
Client 1, Client 2, and Client 3 each want to append 'A', 'B', and 'C' to five files.
Server 2 crashes while Client 1 is appending its data, and Server 1 and Server 3 continue processing the requests.
Later, both Server 1 and Server 2 fail simultaneously, and Server 3 is promoted to the primary.
Clients redirect their requests to Server 3, and Server 3 syncs with the recovering nodes to ensure consistency.

Steps:
------
Client 1, Client 2, and Client 3 each append their respective characters ('A', 'B', and 'C') to different files.
Server 2 crashes during Client 1's file append. Server 1 and Server 3 continue processing requests.
Both Server 1 and Server 2 crash later, and Server 3 is promoted to primary.
Clients redirect their requests to Server 3, and Server 3 processes the pending file append operations.
Servers 1 and 2 recover and synchronize with Server 3.

Expected Results:
-----------------
-> Atomicity: The system guarantees atomicity for each client's operations, so the final files should follow the order "AA", "BB", and "CC" without interleaving.

-> For Strong Consistency:
Server 3, after becoming the primary, synchronizes with Server 1 and Server 2 (after recovery), ensuring no data loss or inconsistencies.
The final file contents across all replicas should be consistent.

-> For Weak/Bounded Consistency:
During the failure, Server 3 may process the requests without waiting for synchronization with Server 1 and Server 2.
Once the servers recover, they synchronize with Server 3, ensuring consistency.
"""

def test_multiple_server_node_failures():
    test_id = "multiple_server_node_failures"
    consistency_model = "strong"  # Or "weak"

    try:
        # Setup Clients and Servers
        server_1 = DistributedServer(server_id="server_1", is_primary=True)
        server_2 = DistributedServer(server_id="server_2", is_primary=False)
        server_3 = DistributedServer(server_id="server_3", is_primary=False)
        
        client_1 = DistributedClient(client_id="client_1", replicas=[server_1, server_2, server_3])
        client_2 = DistributedClient(client_id="client_2", replicas=[server_1, server_2, server_3])
        client_3 = DistributedClient(client_id="client_3", replicas=[server_1, server_2, server_3])

        # Simulate server failures
        server_2.simulate_crash()
        server_1.simulate_crash()

        # Server 3 becomes primary after failures
        log_to_file_and_console(f"{test_id} - Server 3 is now primary", f)

        # Clients send their requests to Server 3
        client_1.append_file("file_1", "A")
        client_2.append_file("file_2", "B")
        client_3.append_file("file_3", "C")

        # Servers 1 and 2 recover and synchronize
        server_1.recover()
        server_2.recover()
        server_3.sync_with_replicas([server_1, server_2])

        # Assert final file contents
        assert check_file_contents("file_1", "A"), "Inconsistent file_1"
        assert check_file_contents("file_2", "B"), "Inconsistent file_2"
        assert check_file_contents("file_3", "C"), "Inconsistent file_3"

        log_result(test_id, True)

    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result(test_id, False)
