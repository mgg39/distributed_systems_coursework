from rpc_connection import RPCConnection

class DistributedClient:
    def __init__(self, client_id, server_address='localhost', server_port=8080):
        self.client_id = client_id
        self.connection = RPCConnection(server_address, server_port)

    def acquire_lock(self):
        message = "acquire_lock"
        print(f"Sending request: {message}")
        result = self.connection.rpc_send(message)
        print(f"LockAcquire Response: {result}")

    def release_lock(self):
        message = "release_lock"
        print(f"Sending request: {message}")
        result = self.connection.rpc_send(message)
        print(f"LockRelease Response: {result}")

    def close(self):
        self.connection.close()

if __name__ == "__main__":
    client = DistributedClient("client_1")
    client.acquire_lock()
    client.release_lock()
    client.close()
