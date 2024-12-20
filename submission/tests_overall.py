import contextlib
import sys
import os
import time
import random
from datetime import datetime
from distributed_client import DistributedClient
from collections import defaultdict

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

os.makedirs(output_folder, exist_ok=True)

def log_to_file_and_console(message, file_handle):
    print(message)
    file_handle.write(message + "\n")
    file_handle.flush()

def log_result(test_name, success, test_id):
    summary_stats[test_name]["total"] += 1
    if success:
        summary_stats[test_name]["success"] += 1
    else:
        summary_stats[test_name]["failed"].append(test_id)

# Network Failure Tests

def network_failure_packet_delay(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", servers=[("localhost", 8080)])
        
        log_to_file_and_console(f"{test_id} - Client 1 attempting to acquire lock", f)
        lock_acquired = client1.acquire_lock()
        log_to_file_and_console(f"{test_id} - Client 1 lock acquired: {lock_acquired}", f)  # Log lock result
        
        if not lock_acquired:
            log_to_file_and_console(f"{test_id} - Client 1 failed to acquire lock", f)
            log_result("network_failure_packet_delay", False, test_id)
            return False
        
        # Client 1 writes "A" into file_1
        result_client1 = client1.append_file("file_1", "A")
        log_to_file_and_console(f"{test_id} - Client 1 appended 'A' to file_1 with result: {result_client1}", f)
        
        # Release the lock for client 2 to take over
        client1.release_lock()
        log_to_file_and_console(f"{test_id} - Client 1 released the lock", f)

        # Allow more time for lock release to propagate
        time.sleep(5)  

        # Client 2 attempts to acquire the lock
        log_to_file_and_console(f"{test_id} - Client 2 attempting to acquire lock", f)
        lock_acquired_client2 = client2.acquire_lock()
        log_to_file_and_console(f"{test_id} - Client 2 lock acquired: {lock_acquired_client2}", f)  # Log lock result
        
        if lock_acquired_client2:
            log_to_file_and_console(f"{test_id} - Client 2 acquired the lock", f)
            result_client2 = client2.append_file("file_1", "B")
            log_to_file_and_console(f"{test_id} - Client 2 appended 'B' to file_1 with result: {result_client2}", f)
            client2.release_lock()
            log_to_file_and_console(f"{test_id} - Client 2 released the lock", f)

        else:
            log_to_file_and_console(f"{test_id} - Client 2 failed to acquire lock", f)
            result_client2 = "failed"

        # Final success check: both clients must have succeeded in appending
        success = result_client1 == "append success" and result_client2 == "append success"
        log_to_file_and_console(f"{test_id} - Test result evaluated: {'PASS' if success else 'FAIL'}", f)
        log_result("network_failure_packet_delay", success, test_id)
        return success

    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("network_failure_packet_delay", False, test_id)
        return False


def network_failure_packet_drop_server_loss(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", servers=[("localhost", 8080)])
        
        log_to_file_and_console(f"{test_id} - Simulating server packet loss for client 1 lock acquisition", f)
        
        # Client 1 acquires lock and writes "A" to file_1
        client1.acquire_lock()
        result_client1 = client1.append_file("file_1", "A")
        log_to_file_and_console(f"{test_id} - Client 1 appended 'A' to file_1 with result: {result_client1}", f)
        client1.release_lock()
        
        # Client 2 acquires lock and writes "B" to file_1
        lock_acquired_client2 = client2.acquire_lock()
        if lock_acquired_client2:
            result_client2 = client2.append_file("file_1", "B")
            log_to_file_and_console(f"{test_id} - Client 2 appended 'B' to file_1 with result: {result_client2}", f)
            result_client2 == "append success"
            client2.release_lock()
        else:
            log_to_file_and_console(f"{test_id} - Client 2 failed to acquire lock", f)
            result_client2 = "failed"
        
        # Success condition: both appends should be successful
        success = result_client1 == "append success" and result_client2 == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("network_failure_packet_drop_server_loss", success, test_id)
        return success

    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("network_failure_packet_drop_server_loss", False, test_id)
        return False

def network_failure_packet_drop_client_loss(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", servers=[("localhost", 8080)])
        
        log_to_file_and_console(f"{test_id} - Simulating client 1 packet loss", f)
        lock_acquired_client2 = client2.acquire_lock()
        result = client2.append_file("file_1", "B")  # Client 2 writes "B" into file_1
        
        client2.release_lock()
        lock_acquired_client1 = client1.acquire_lock()
        result = client1.append_file("file_1", "A")  # Client 1 writes "A" into file_1
        
        success = result == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("network_failure_packet_drop_client_loss", success, test_id)
        return success
    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("network_failure_packet_drop_client_loss", False, test_id)
        return False

def network_failure_duplicated_packets(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", servers=[("localhost", 8080)])
        
        log_to_file_and_console(f"{test_id} - Client 1 acquiring lock and appending", f)
        client1.acquire_lock()
        client1.append_file("file_1", "A")  # Client 1 writes "A" into file_1
        
        client1.release_lock()
        log_to_file_and_console(f"{test_id} - Simulating duplicated release", f)
        client1.release_lock() 
        
        lock_acquired_client2 = client2.acquire_lock()
        result = client2.append_file("file_1", "B")  # Client 2 writes "B" into file_1
        
        success = result == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("network_failure_duplicated_packets", success, test_id)
        return success
    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("network_failure_duplicated_packets", False, test_id)
        return False

def network_failure_combined_failures(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", servers=[("localhost", 8080)])
        
        log_to_file_and_console(f"{test_id} - Client 1 acquiring lock and appending", f)
        client1.acquire_lock()
        client1.append_file("file_1", "A")  # Client 1 writes "A" into file_1
        
        client1.release_lock()
        lock_acquired_client2 = client2.acquire_lock()
        result = client2.append_file("file_1", "B")  # Client 2 writes "B" into file_1
        
        success = result == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("network_failure_combined_failures", success, test_id)
        return success
    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("network_failure_combined_failures", False, test_id)
        return False

# Client Failure Tests

def client_failure_stall_before_edit(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", servers=[("localhost", 8080)])
        
        # Client 1 acquires lock, waits to simulate stall, then releases
        client1.acquire_lock()
        time.sleep(LOCK_LEASE_DURATION + 1)
        client1.release_lock()
        log_to_file_and_console(f"{test_id} - Client 1 stalled and released lock", f)

        # Client 2 acquires lock and writes "B" to file_1
        lock_acquired_client2 = client2.acquire_lock()
        if lock_acquired_client2:
            result_client2 = client2.append_file("file_1", "B")
            log_to_file_and_console(f"{test_id} - Client 2 appended 'B' to file_1 with result: {result_client2}", f)
        else:
            log_to_file_and_console(f"{test_id} - Client 2 failed to acquire lock", f)
            result_client2 = "failed"
        
        # Success condition: client 2 should succeed
        success = result_client2 == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("client_failure_stall_before_edit", success, test_id)
        return success

    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("client_failure_stall_before_edit", False, test_id)
        return False

def client_failure_stall_after_edit(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", servers=[("localhost", 8080)])
        
        client1.acquire_lock()
        client1.append_file("file_1", "A")  # Client 1 writes "A" into file_1
        time.sleep(LOCK_LEASE_DURATION + 1)
        
        lock_acquired_client2 = client2.acquire_lock()
        result = client2.append_file("file_1", "B")  # Client 2 writes "B" into file_1
        
        success = result == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("client_failure_stall_after_edit", success, test_id)
        return success
    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("client_failure_stall_after_edit", False, test_id)
        return False

# Server Failure Tests

def single_server_failure_lock_free(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        
        client1.acquire_lock()
        client1.append_file("file_1", "A")  # Client 1 writes "A" into file_1
        
        client1.release_lock()
        time.sleep(2)
        
        client1.acquire_lock()
        result = client1.append_file("file_1", "A")  # Client 1 writes "A" again into file_1
        
        success = result == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("single_server_failure_lock_free", success, test_id)
        return success
    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("single_server_failure_lock_free", False, test_id)
        return False

def single_server_failure_lock_held(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", servers=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", servers=[("localhost", 8080)])
        
        # Client 1 acquires lock and writes "A" to file_1
        client1.acquire_lock()
        result_client1 = client1.append_file("file_1", "A")
        log_to_file_and_console(f"{test_id} - Client 1 appended 'A' to file_1 with result: {result_client1}", f)
        client1.release_lock()
        
        # Client 2 acquires lock and writes "B" to file_1
        lock_acquired_client2 = client2.acquire_lock()
        if lock_acquired_client2:
            result_client2 = client2.append_file("file_1", "B")
            log_to_file_and_console(f"{test_id} - Client 2 appended 'B' to file_1 with result: {result_client2}", f)
            client2.release_lock()
            result_client2 == "append success"
        else:
            log_to_file_and_console(f"{test_id} - Client 2 failed to acquire lock", f)
            result_client2 = "failed"
        
        # Success condition: both appends should be successful
        success = result_client1 == "append success" and result_client2 == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("single_server_failure_lock_held", success, test_id)
        return success

    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("single_server_failure_lock_held", False, test_id)
        return False

# Run 

summary_stats = defaultdict(lambda: {"total": 0, "success": 0, "failed": []})

def run_test(test_function, test_name, iterations):
    for i in range(iterations):
        test_id = f"{test_name}_{i+1}"  # Unique test ID per iteration
        output_file = os.path.join(output_folder, f"{test_id}.txt")
        
        with open(output_file, "w") as f:
            log_to_file_and_console(f"Test ID: {test_id}", f)
            log_to_file_and_console(f"Test: {test_name}", f)
            log_to_file_and_console(f"Iteration: {i+1}", f)
            log_to_file_and_console(f"Start Time: {datetime.now()}", f)
            
            # Run the test function and log detailed output
            success = test_function(test_id, f)
            result = "PASS" if success else "FAIL"
            
            log_to_file_and_console(f"Result: {result}", f)
            log_to_file_and_console(f"End Time: {datetime.now()}", f)

def main():
    # Clear previous test results
    if os.path.exists(summary_file):
        os.remove(summary_file)
    
    if os.path.exists(output_folder):
        for filename in os.listdir(output_folder):
            file_path = os.path.join(output_folder, filename)
            os.remove(file_path)
    
    with open(summary_file, "a") as summary:
        summary.write("Test Summary\n")
        summary.write("====================\n")
        
        test_functions = [
            (network_failure_packet_delay, "network_failure_packet_delay"), #simulated: 100ms delay tc
            (network_failure_packet_drop_server_loss, "network_failure_packet_drop"), #simulated: 1% loss tc
            (client_failure_stall_before_edit, "client_failure_stall_before_edit"), 
            (client_failure_stall_after_edit, "client_failure_stall_after_edit"), 
            (single_server_failure_lock_free, "single_server_failure_lock_free"), 
            (single_server_failure_lock_held, "single_server_failure_lock_held"), 
        ]
        for test_func, test_name in test_functions:
            run_test(test_func, test_name, 1)

        # Generate summary report
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

"""
sudo tc qdisc add dev wlp0s20f3 root netem delay 100ms loss 30%
sudo tc -s qdisc

-run python

sudo tc qdisc del dev wlp0s20f3 root

"""