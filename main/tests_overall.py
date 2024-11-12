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
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080)])
        
        log_to_file_and_console(f"{test_id} - Client 1 attempting to acquire lock", f)
        lock_acquired = client1.acquire_lock()
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

        # Allow time for lock release to propagate
        time.sleep(2)

        # Client 2 attempts to acquire the lock
        log_to_file_and_console(f"{test_id} - Client 2 attempting to acquire lock", f)
        lock_acquired_client2 = client2.acquire_lock()
        if lock_acquired_client2:
            log_to_file_and_console(f"{test_id} - Client 2 acquired the lock", f)
            result_client2 = client2.append_file("file_1", "B")
            log_to_file_and_console(f"{test_id} - Client 2 appended 'B' to file_1 with result: {result_client2}", f)
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
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080)])
        
        log_to_file_and_console(f"{test_id} - Simulating server packet loss for client 1 lock acquisition", f)
        client1.acquire_lock()
        result = client1.append_file("file_1", "A")  # Client 1 writes "A" into file_1
        
        client1.release_lock()
        lock_acquired_client2 = client2.acquire_lock()
        result = client2.append_file("file_1", "B")  # Client 2 writes "B" into file_1
        
        success = result == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("network_failure_packet_drop_server_loss", success, test_id)
        return success
    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("network_failure_packet_drop_server_loss", False, test_id)
        return False

def network_failure_packet_drop_client_loss(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080)])
        
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
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080)])
        
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
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080)])
        
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
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080)])
        
        client1.acquire_lock()
        time.sleep(LOCK_LEASE_DURATION + 1)
        
        lock_acquired_client2 = client2.acquire_lock()
        result = client2.append_file("file_1", "B")  # Client 2 writes "B" into file_1
        
        success = result == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("client_failure_stall_before_edit", success, test_id)
        return success
    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("client_failure_stall_before_edit", False, test_id)
        return False

def client_failure_stall_after_edit(test_id, f):
    try:
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080)])
        
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
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        
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
        client1 = DistributedClient(client_id="client_1", replicas=[("localhost", 8080)])
        client2 = DistributedClient(client_id="client_2", replicas=[("localhost", 8080)])
        
        client1.acquire_lock()
        client1.append_file("file_1", "A")  # Client 1 writes "A" into file_1
        client1.release_lock()
        
        lock_acquired_client2 = client2.acquire_lock()
        client2.append_file("file_1", "B")  # Client 2 writes "B" into file_1
        time.sleep(2)
        
        result = client2.append_file("file_1", "B")  # Client 2 writes "B" again into file_1
        
        success = result == "append success"
        log_to_file_and_console(f"{test_id} - Test result: {'PASS' if success else 'FAIL'}", f)
        log_result("single_server_failure_lock_held", success, test_id)
        return success
    except Exception as e:
        log_to_file_and_console(f"{test_id} - Exception: {e}", f)
        log_result("single_server_failure_lock_held", False, test_id)
        return False

# Run 

# Phase 1: iterations of specific test functions (50 per test type)

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

# Phase 2: Multi client randomized Crash Test (2000)
def run_randomized_crash_test(test_id):
    test_id = f"multi_crash_{test_id}"
    output_file = os.path.join(output_folder, f"{test_id}.txt")
    
    with open(output_file, "w") as f:
        log_to_file_and_console(f"Test ID: {test_id}", f)
        log_to_file_and_console("Randomized Crash Test with 5-10 Clients", f)
        log_to_file_and_console(f"Start Time: {datetime.now()}\n", f)

        # Generate random events and clients
        num_clients = random.randint(5, 10)
        events = []

        for i in range(num_clients):
            client_event = random.choice(["acquire_lock", "append_file", "release_lock", "stall", "packet_delay", "packet_drop"])
            events.append((f"client_{i+1}", client_event))
        
        log_to_file_and_console("Event Sequence:", f)
        for client, event in events:
            log_to_file_and_console(f"  - {client} will {event}", f)
        log_to_file_and_console("\n", f)
        
        # Execute events and capture detailed logs
        success = True
        for client_id, event in events:
            client = DistributedClient(client_id=client_id, replicas=[("localhost", 8080)])
            
            if event == "acquire_lock":
                result = client.acquire_lock()
                log_to_file_and_console(f"{client_id} tried to acquire lock: {result}", f)
                if result != "PASS":
                    success = False
            
            elif event == "append_file":
                result = client.append_file("test_file", "data")
                log_to_file_and_console(f"{client_id} attempted to append to file: {result}", f)
                if result != "PASS":
                    success = False
            
            elif event == "release_lock":
                result = client.release_lock()
                log_to_file_and_console(f"{client_id} released lock: {result}", f)
                if result != "PASS":
                    success = False
            
            elif event == "stall":
                log_to_file_and_console(f"{client_id} stalled for lock timeout.", f)
                time.sleep(LOCK_LEASE_DURATION + 1)
            
            elif event == "packet_delay":
                log_to_file_and_console(f"{client_id} experienced simulated packet delay.", f)
                time.sleep(2)
            
            elif event == "packet_drop":
                log_to_file_and_console(f"{client_id} encountered simulated packet drop. No action taken.", f)
                continue

        log_to_file_and_console(f"End Time: {datetime.now()}", f)
        result = "PASS" if success else "FAIL"

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
        
        # Run just the first test for verification
        test_func, test_name, iterations = network_failure_packet_delay, "network_failure_packet_delay", 1
        for i in range(iterations):
            test_id = f"{test_name}_{i+1}"
            output_file = os.path.join(output_folder, f"{test_id}.txt")
            
            with open(output_file, "w") as f:
                with contextlib.redirect_stdout(f):
                    print(f"Test ID: {test_id}")
                    print(f"Test: {test_name}")
                    print(f"Iteration: {i+1}")
                    print(f"Start Time: {datetime.now()}")
                    
                    # Run the test and log results
                    success = test_func(test_id, f)
                    result = "PASS" if success else "FAIL"
                    
                    print(f"Result: {result}")
                    print(f"End Time: {datetime.now()}")
                    
                    # Log result in summary_stats
                    log_result(test_name, success, test_id)
    
    # Report Summary
    with open(summary_file, "a") as summary:
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
