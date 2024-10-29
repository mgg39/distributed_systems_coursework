import grpc
import grpc_lock_manager.lock_manager_pb2 as lock_manager_pb2
import grpc_lock_manager.lock_manager_pb2_grpc as lock_manager_pb2_grpc

class DistributedClient:
    def __init__(self, client_id):
        self.client_id = client_id
        self.channel = grpc.insecure_channel('localhost:8080')
        self.stub = lock_manager_pb2_grpc.LockManagerStub(self.channel)

    def acquire_lock(self):
        request = lock_manager_pb2.LockRequest(client_id=self.client_id)
        response = self.stub.LockAcquire(request)
        print(f"LockAcquire Response: {response.message}")

    def append_file(self, file_name, data):
        request = lock_manager_pb2.FileAppendRequest(
            client_id=self.client_id, file_name=file_name, data=data)
        response = self.stub.AppendFile(request)
        print(f"AppendFile Response: {response.message}")

    def release_lock(self):
        request = lock_manager_pb2.LockRequest(client_id=self.client_id)
        response = self.stub.LockRelease(request)
        print(f"LockRelease Response: {response.message}")

    def close(self):
        self.channel.close()

# Example usage
if __name__ == "__main__":
    client = DistributedClient("client_1")
    
    # Try acquire lock
    client.acquire_lock()
    
    # Append data to file
    client.append_file("file_0", "This is a test log entry.")
    
    # Release lock
    client.release_lock()
    
    # Close channel
    client.close()
