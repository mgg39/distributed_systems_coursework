# client.py

from rpc_connection import RPCConnection

# Initialize connection with the server's IP address and port
rpc_conn = RPCConnection('127.0.0.1', 8080)
result = rpc_conn.rpc_init()

# Check if the connection was successful
if result == 0:
    print("Connection successful!")
else:
    print("Failed to connect to the server.")

# Close the connection when done
rpc_conn.close()