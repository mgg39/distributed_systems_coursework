import grpc
from concurrent import futures
import time
import threading
import os
import grpc_lock_manager.lock_manager_pb2 as lock_manager_pb2
import grpc_lock_manager.lock_manager_pb2_grpc as lock_manager_pb2_grpc

# Configuration
FILES_DIR = "server_files"
lock = threading.Lock()
current_lock_holder = None

# If directory for files exists -> create 100 files
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)
for i in range(100):
    open(os.path.join(FILES_DIR, f"file_{i}"), 'a').close()

class LockManagerServicer(lock_manager_pb2_grpc.LockManagerServicer):
    def LockAcquire(self, request):
        global current_lock_holder
        with lock:
            if current_lock_holder is None:
                current_lock_holder = request.client_id
                return lock_manager_pb2.LockResponse(message="Lock acquired")
            else:
                return lock_manager_pb2.LockResponse(message="Lock is currently held")

    def LockRelease(self, request):
        global current_lock_holder
        with lock:
            if current_lock_holder == request.client_id:
                current_lock_holder = None
                return lock_manager_pb2.LockResponse(message="Lock released")
            else:
                return lock_manager_pb2.LockResponse(message="You do not hold the lock")

    def AppendFile(self, request):
        global current_lock_holder
        with lock:
            if current_lock_holder == request.client_id:
                file_path = os.path.join(FILES_DIR, request.file_name)
                if os.path.exists(file_path):
                    with open(file_path, 'a') as f:
                        f.write(request.data + "\n")
                    return lock_manager_pb2.FileAppendResponse(message="Data appended to file")
                else:
                    return lock_manager_pb2.FileAppendResponse(message="File not found")
            else:
                return lock_manager_pb2.FileAppendResponse(message="You do not hold the lock")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lock_manager_pb2_grpc.add_LockManagerServicer_to_server(LockManagerServicer(), server)
    server.add_insecure_port('[::]:8080')
    server.start()
    print("Server listening on port 8080.")
    try:
        while True:
            time.sleep(86400)  # Kserver runs indefinitely
    except KeyboardInterrupt:
        server.stop(0)
        print("Server stopped.")

if __name__ == "__main__":
    serve()
