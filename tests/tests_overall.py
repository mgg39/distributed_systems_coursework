import os
import time
from datetime import datetime
from distributed_client import DistributedClient

LOCK_LEASE_DURATION = 10  

output_folder = "test_results"
summary_file = "test_summary.txt"

summary_stats = {
    "network_failure_packet_delay": {"total": 0, "success": 0, "failed": []},
    "network_failure_packet_drop": {"total": 0, "success": 0, "failed": []},
    "client_failure_stall_before_edit": {"total": 0, "success": 0, "failed": []},
    "client_failure_stall_after_edit": {"total": 0, "success": 0, "failed": []},
    "single_server_failure_lock_free": {"total": 0, "success": 0, "failed": []},
    "single_server_failure_lock_held": {"total": 0, "success": 0, "failed": []},
}

# output folder
os.makedirs(output_folder, exist_ok=True)

# Helper functions for logging results
def log_result(test_name, success, test_id):
    summary_stats[test_name]["total"] += 1
    if success:
        summary_stats[test_name]["success"] += 1
    else:
        summary_stats[test_name]["failed"].append(test_id)

# Test cases
#Network ----------------------------------------------------------------------------------

def network_failure_packet_delay(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8081)])
        
        # Client 1 acquires the lock
        lock_acquired = client1.acquire_lock()
        if not lock_acquired:
            log_result("network_failure_packet_delay", False, test_id)
            return False  # Fail - lock not acquired
        
        # network delay
        time.sleep(LOCK_LEASE_DURATION + 1)  # Wait for lock to expire
        
        # Attempt to append (should fail - lock expired)
        result = client1.append_file("file_A", "data_A")
        success = "ERROR: LOCK EXPIRED" in result  # Adjust based error message
        
        # Client 2 tries to acquire the lock after Client 1's lock expires
        lock_acquired_client2 = client2.acquire_lock()
        if not lock_acquired_client2:
            log_result("network_failure_packet_delay", False, test_id)
            return False  # Fail if Client 2 couldn’t acquire the lock
        
        log_result("network_failure_packet_delay", success, test_id)
        return success
    except Exception as e:
        # Log the exception as failure
        print(f"Exception in {test_id}: {e}")
        log_result("network_failure_packet_delay", False, test_id)
        return False
    
def network_failure_packet_drop_server_loss(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8081)])
        
        # Simulate server packet loss by not receiving initial lock confirmation for client1
        client1.acquire_lock()  # Assume server "drops" response
        client1.append_file("file_A", "data_A")  # Retry after confirming lock ownership
        
        # client2 acquires and appends after client1 releases the lock
        client1.release_lock()
        client2.acquire_lock()
        result = client2.append_file("file_B", "data_B")
        
        # Check for sequence "AB" in the file
        success = result == "PASS"  # Adjust based on expected server output format
        log_result("network_failure_packet_drop_server_loss", success, test_id)
        return success
    except Exception as e:
        log_result("network_failure_packet_drop_server_loss", False, test_id)
        return False
    
def network_failure_packet_drop_client_loss(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8081)])
        
        # Simulate packet drop by skipping client1's initial acquire attempt
        client2.acquire_lock()
        result = client2.append_file("file_B", "data_B")
        
        # client1 retries and acquires the lock after client2 releases it
        client2.release_lock()
        client1.acquire_lock()
        result = client1.append_file("file_A", "data_A")
        
        # Check for sequence "BA" in the file
        success = result == "PASS"  # Adjust based on expected server output format
        log_result("network_failure_packet_drop_client_loss", success, test_id)
        return success
    except Exception as e:
        log_result("network_failure_packet_drop_client_loss", False, test_id)
        return False

def network_failure_duplicated_packets(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8081)])
        
        client1.acquire_lock()
        client1.append_file("file_A", "data_A")
        
        # Simulate duplicate `release_lock` call
        client1.release_lock()
        client1.release_lock()  # Duplicate
        
        # client2 acquires lock and appends
        client2.acquire_lock()
        result = client2.append_file("file_B", "data_B")
        
        # Check for "ABBA" sequence in the file
        success = result == "PASS"  # Adjust based on expected server output format
        log_result("network_failure_duplicated_packets", success, test_id)
        return success
    except Exception as e:
        log_result("network_failure_duplicated_packets", False, test_id)
        return False

def network_failure_combined_failures(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8081)])
        
        client1.acquire_lock()
        client1.append_file("file_1", "data_1")
        
        # Simulate lost append for "A" and retry
        client1.append_file("file_A", "data_A")
        
        # client1 releases, client2 acquires and appends
        client1.release_lock()
        client2.acquire_lock()
        result = client2.append_file("file_B", "data_B")
        
        # Expect "1AB" in file
        success = result == "PASS"  # Adjust based on expected server output format
        log_result("network_failure_combined_failures", success, test_id)
        return success
    except Exception as e:
        log_result("network_failure_combined_failures", False, test_id)
        return False

#Client ----------------------------------------------------------------------------------

def client_failure_stall_before_edit(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8081)])
        
        client1.acquire_lock()
        time.sleep(LOCK_LEASE_DURATION + 1)  # Simulate stall that exceeds lease duration
        
        # Client 2 acquires the lock after timeout and appends
        client2.acquire_lock()
        result = client2.append_file("file_B", "data_B")
        
        success = result == "PASS"  # Adjust based on expected server output format
        log_result("client_failure_stall_before_edit", success, test_id)
        return success
    except Exception as e:
        log_result("client_failure_stall_before_edit", False, test_id)
        return False

def client_failure_stall_after_edit(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8081)])
        
        client1.acquire_lock()
        client1.append_file("file_A", "data_A")
        time.sleep(LOCK_LEASE_DURATION + 1)  # Simulate stall that exceeds lease duration
        
        # Client 2 acquires and appends after client1 times out
        client2.acquire_lock()
        result = client2.append_file("file_B", "data_B")
        
        success = result == "PASS"  # Adjust based on expected server output format
        log_result("client_failure_stall_after_edit", success, test_id)
        return success
    except Exception as e:
        log_result("client_failure_stall_after_edit", False, test_id)
        return False

#Server ----------------------------------------------------------------------------------

def single_server_failure_lock_free(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        
        client1.acquire_lock()
        client1.append_file("file_A", "data_A")
        
        # Simulate server restart
        client1.release_lock()
        time.sleep(2)  # Allow time for server to "recover"
        
        # client1 re-acquires and appends
        client1.acquire_lock()
        result = client1.append_file("file_1", "data_1")
        
        success = result == "PASS"  # Adjust based on expected server output format
        log_result("single_server_failure_lock_free", success, test_id)
        return success
    except Exception as e:
        log_result("single_server_failure_lock_free", False, test_id)
        return False

def single_server_failure_lock_held(test_id):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8081)])
        
        client1.acquire_lock()
        client1.append_file("file_A", "data_A")
        client1.release_lock()
        
        # client2 acquires lock and appends, then server "crashes and recovers"
        client2.acquire_lock()
        client2.append_file("file_B", "data_B")
        time.sleep(2)  # Simulate server recovery
        
        result = client2.append_file("file_B", "data_B")
        
        # Expected file sequence: "ABBA"
        success = result == "PASS"  # Adjust based on expected server output format
        log_result("single_server_failure_lock_held", success, test_id)
        return success
    except Exception as e:
        log_result("single_server_failure_lock_held", False, test_id)
        return False

# Run 
def run_test(test_function, test_name, iterations):
    for i in range(iterations):
        test_id = f"{test_name}_{i+1}"
        output_file = os.path.join(output_folder, f"{test_id}.txt")
        
        # Log
        with open(output_file, "w") as f:
            f.write(f"Test: {test_name}\n")
            f.write(f"Iteration: {i+1}\n")
            f.write(f"Start Time: {datetime.now()}\n")
            
            success = test_function(test_id)
            result = "PASS" if success else "FAIL"
            
            f.write(f"Result: {result}\n")
            f.write(f"End Time: {datetime.now()}\n")

def main():
    run_test(network_failure_packet_delay, "network_failure_packet_delay", 50)
    run_test(network_failure_packet_drop_client_loss, "network_failure_packet_drop_client_loss", 50)
    run_test(network_failure_packet_drop_server_loss, "network_failure_packet_drop_server_loss", 50)
    run_test(network_failure_duplicated_packets, "network_failure_duplicated_packets", 50)
    run_test(network_failure_combined_failures, "network_failure_combined_failures", 50)
    
    run_test(client_failure_stall_before_edit, "client_failure_stall_before_edit", 50)
    run_test(client_failure_stall_after_edit, "client_failure_stall_after_edit", 50)
    
    run_test(single_server_failure_lock_free, "single_server_failure_lock_free", 50)
    run_test(single_server_failure_lock_held, "single_server_failure_lock_held", 50)

    with open(summary_file, "w") as summary:
        summary.write("Test Summary\n")
        summary.write("====================\n")
        for test_name, stats in summary_stats.items():
            summary.write(f"{test_name}:\n")
            summary.write(f"  Total Runs: {stats['total']}\n")
            summary.write(f"  Successes: {stats['success']}\n")
            summary.write(f"  Failures: {len(stats['failed'])}\n")
            if stats["failed"]:
                summary.write("  Failed Test IDs:\n")
                for failed_test in stats["failed"]:
                    summary.write(f"    - {failed_test}\n")
            summary.write("\n")

if __name__ == "__main__":
    main()
