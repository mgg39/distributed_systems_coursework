'''
This files tests that:
    - server can restore its state after a restart,
    - client can reconnect and renew/re-acquire the lock as expected

Make sure to comment out line 126 of test_dc_simulated_crashes to prevent the client from releasing the lock!
'''
import time
import subprocess
import json
import signal

# Constants
LOCK_LEASE_DURATION = 10  # Match the lease duration on the server
STATE_FILE = "server_state.json"

# Helper function to clean up any existing server instances on the port
def cleanup_existing_server():
    try:
        subprocess.call("fuser -k 8080/udp", shell=True)
    except Exception as e:
        print(f"Error cleaning up existing server: {e}")

# Step 1: Start the server and client, acquire the lock
cleanup_existing_server()

# Start the server as a subprocess
server_process = subprocess.Popen(["python3", "upd_server_copy.py"])
time.sleep(2)  # Allow server time to start

# Start the client as a subprocess to acquire the lock
client_process = subprocess.Popen(["python3", "dc_simulated_crashes.py"])
time.sleep(5)  # Wait for client to acquire lock

# Check that the lock has been acquired by the client
with open(STATE_FILE, "r") as f:
    state_before_restart = json.load(f)
    print(f"State before restart: {state_before_restart}")  # Debug print
    assert state_before_restart["current_lock_holder"] == "client_1", "Client should have acquired the lock."

print("Step 1 passed: Client acquired lock successfully.")

# Step 2: Restart the server
print("Restarting the server to test persistence...")

# Terminate the server to simulate a crash
server_process.terminate()
server_process.wait()  # Wait for server to fully terminate

# Restart the server
server_process = subprocess.Popen(["python3", "upd_server.py"])
time.sleep(2)  # Allow server time to restart

# Check that the server restored its state
with open(STATE_FILE, "r") as f:
    state_after_restart = json.load(f)
    print(f"State after restart: {state_after_restart}")  # Debug print
    assert state_after_restart["current_lock_holder"] == "client_1", "Server should have restored the lock state."

print("Step 2 passed: Server restarted and restored state successfully.")

# Step 3: Reconnect the client to verify it can renew its lease
print("Attempting client reconnect...")

# Terminate the client to simulate a disconnect
client_process.terminate()  
time.sleep(2)  # Give time for process termination

# Re-run the client to simulate reconnection
client_process = subprocess.Popen(["python3", "dc_simulated_crashes.py"])
time.sleep(5)  # Wait for client to reconnect and renew the lease

# Final verification of lock state after client reconnect
with open(STATE_FILE, "r") as f:
    final_state = json.load(f)
    print(f"Final state after client reconnect: {final_state}")  # Debug print
    assert final_state["current_lock_holder"] == "client_1", "Client should hold the lock after reconnect."

print("Test passed: Client successfully reconnected and lock state is consistent.")

# Clean up: Terminate server and client processes
client_process.terminate()
server_process.terminate()
print("Cleanup completed.")
