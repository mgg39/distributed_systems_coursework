'''
This file tests if the server handles unexpected client disconnections gracefully, ensuring the lock is not held indefinitely.
'''
import os
import time
import subprocess
import signal
import json

# Constants
LOCK_LEASE_DURATION = 10  # Match the lease duration on the server

# Start server as a subprocess
server_process = subprocess.Popen(["python3", "upd_server_copy.py"])

# Allow server to start
time.sleep(2)

# Start client as a subprocess
client_process = subprocess.Popen(["python3", "dc_simulated_crashes.py"])

# Give the client time to acquire the lock
time.sleep(5)

# Simulate client crash by forcefully terminating the client process
client_process.terminate()  # Send SIGTERM for a soft exit; adjust to SIGKILL if needed

# Wait to give time for the lock to expire
time.sleep(LOCK_LEASE_DURATION + 2)  # Wait slightly longer than the lease duration

# Check server_state.json to confirm the lock was released by the server
with open("server_state.json", "r") as f:
    server_state = json.load(f)
    assert server_state["current_lock_holder"] is None, "Lock should be released after client crash."

# Clean up server process
server_process.terminate()
print("Test passed: Server released lock after client crash.")
