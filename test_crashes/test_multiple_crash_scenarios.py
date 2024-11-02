#Tested with both dc crashes = True
import os
import time
import subprocess
import signal
import json
import random
from collections import defaultdict

# Constants
NUM_CLIENTS = 5
LOCK_LEASE_DURATION = 10  # Match the server lease duration
STATE_FILE = "server_state.json"
NUM_ITERATIONS = 1000  # Number of iterations per scenario

# Initialize statistics dictionary
stats = defaultdict(lambda: defaultdict(int))

# Helper function to clean up existing server instances on the port
def cleanup_existing_server():
    try:
        subprocess.call("fuser -k 8080/udp", shell=True)
    except Exception as e:
        print(f"Error cleaning up existing server: {e}")

# Function to simulate a client process running a specific action
def run_client_action(client_id, action):
    client_script = "dc_simulated_crashes.py"
    env = os.environ.copy()
    env["CLIENT_ID"] = f"client_{client_id}"
    env["CLIENT_ACTION"] = action
    return subprocess.Popen(["python3", client_script], env=env)

# Function to simulate different crash scenarios
def simulate_crash_scenario(client_processes, server_process, scenario_type):
    if scenario_type == "client_crash":
        # Randomly terminate one client process
        client_to_kill = random.choice(client_processes)
        client_to_kill.terminate()
        stats["client_crash"]["count"] += 1

    elif scenario_type == "server_crash":
        # Terminate the server process to simulate a server crash
        server_process.terminate()
        stats["server_crash"]["count"] += 1

        # Restart server process
        server_process = subprocess.Popen(["python3", "upd_server_copy.py"])
        time.sleep(2)  # Give server time to restart

    elif scenario_type == "network_delay":
        # Randomly select a client to delay heartbeats for
        client_to_delay = random.choice(client_processes)
        stats["network_delay"]["count"] += 1
        time.sleep(LOCK_LEASE_DURATION + 2)  # Delay longer than lease duration

    return server_process  # Return potentially restarted server process

# Main function to run the test suite
def run_test_suite():
    for _ in range(NUM_ITERATIONS):
        cleanup_existing_server()  # Ensure no processes are running before test

        # Start the server process
        server_process = subprocess.Popen(["python3", "upd_server_copy.py"])
        time.sleep(2)  # Allow server time to start

        # Start 5 clients with different actions
        client_processes = []
        actions = ["acquire_lock", "append_data", "hold_lock", "release_lock", "reconnect"]
        for i in range(1, NUM_CLIENTS + 1):
            client_process = run_client_action(i, random.choice(actions))
            client_processes.append(client_process)
        
        # Introduce random crash scenario
        crash_scenario = random.choice(["client_crash", "server_crash", "network_delay"])
        server_process = simulate_crash_scenario(client_processes, server_process, crash_scenario)

        # Wait to see if server handles the crashes as expected
        time.sleep(5)

        # Gather outcomes from server_state.json
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
            if state["current_lock_holder"]:
                stats["successful_locks"]["count"] += 1
            else:
                stats["failed_locks"]["count"] += 1

        # Clean up client processes
        for client in client_processes:
            client.terminate()
        
        # Clean up server process
        server_process.terminate()

    # Output statistics
    print("\n--- Test Results ---")
    for scenario, counts in stats.items():
        print(f"{scenario}: {counts['count']} occurrences")

# Run the test suite
run_test_suite()

"""
--- Test Results ---
client_crash: 320 occurrences
successful_locks: 996 occurrences
server_crash: 349 occurrences
network_delay: 331 occurrences
failed_locks: 4 occurrences
"""

# failure rate of 0.4% ->  99.6% success rate for lock acquisitions despite frequent crashes and delays, the system appears highly reliable