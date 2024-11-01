# Distributed Lock Server

This coursework implements a UDP-based lock server for a distributed system as part of the Distributed Systems course at the University of Edinburgh. The server allows multiple clients to safely acquire and release locks on shared resources, demonstrating key concepts of concurrency and fault tolerance.

## Table of Contents

- [Overview](#overview)
- [File Structure](#file-structure)
- [Implementation Details](#implementation-details)
  - [Network Failure and Client/Server Crash Handling](#network-failure-and-clientserver-crash-handling)
  - [RPC Initialization and Lock Management](#rpc-initialization-and-lock-management)
  - [Socket Programming and Multithreading](#socket-programming-and-multithreading)
  - [Parsing and __pycache__](#parsing-and-pycache)
- [Usage](#usage)
- [Dependencies](#dependencies)
- [Environment](#environment)
- [Authors](#authors)

## Overview

The system consists of a lock manager server and multiple distributed clients that communicate with the server to manage access to shared files. Clients must acquire a lock before they can append data to a file, ensuring that only one client can modify a file at a time.

## File Structure

The project contains the following main files:

1. **`upd_server.py`**: Implements the UDP lock server.
2. **`rpc_connection.py`**: Handles RPC connections between clients and the server.
3. **`distributed_client.py`**: Represents the client that interacts with the server to manage locks and append data to files.
4. **`test_client.py`**: Contains a testing framework to simulate multiple clients attempting to acquire locks and write to files.

## Implementation Details

### Network Failure and Client/Server Crash Handling

- **Lock Expiration**: The server uses a lease mechanism for locks, where a lock is automatically released if not renewed within a specified duration (`LOCK_LEASE_DURATION`). This prevents locks from being held indefinitely due to client crashes.
- **Heartbeat Mechanism**: Clients send periodic heartbeat messages to renew their locks. If a client fails to send a heartbeat before the lease expires, the server automatically releases the lock.
- **State Persistence**: The server's state is saved to a JSON file (`server_state.json`) on startup and shutdown, allowing it to recover its state after crashes.

### RPC Initialization and Lock Management

- **RPC Connection**: The `RPCConnection` class in `rpc_connection.py` manages UDP communication with the server. It implements a timeout mechanism for request handling.
- **Lock Management**: The server supports three main operations for lock management:
  - `acquire_lock`: Clients request locks and receive a lease duration if granted.
  - `release_lock`: Clients release locks they hold.
  - `append_file`: Clients append data to files only if they hold the lock.

### Socket Programming and Multithreading

- The server is implemented using Python’s `socket` library for UDP communication. It listens for client requests in a loop, spawning new threads to handle each request, allowing multiple clients to interact with the server simultaneously.
- Multithreading is used for both the server and client implementations. The server runs a background thread to monitor lock expiration, while clients run threads to send heartbeat messages without blocking their main operations.

#### Parsing and `__pycache__`

##### Parsing

Parsing is a crucial aspect of the communication between the clients and the server in this distributed system. It involves analyzing incoming messages and extracting relevant information to determine the appropriate actions to take. Here are key points about parsing in the project:

1. **Message Structure**: The communication between clients and the server follows a predefined message format:
    `<message_type>:<client_id>:<request_id>:<additional_info>`
    This structure enables clear and organized communication, ensuring that each part of the message is easily identifiable.

2. **Message Handling**: 
- In the server's `handle_request` method, incoming messages are parsed using the `split` method to separate the components based on the colon (`:`) delimiter. This allows the server to efficiently retrieve the `message_type`, `client_id`, `request_id`, and any additional information (such as the file name and data to append).
- Clients also utilize parsing when constructing messages to ensure they conform to the expected format before sending them to the server.

3. **Error Handling**: The parsing process is accompanied by robust error handling to manage cases where the message format is invalid or incomplete. This ensures that the server can respond appropriately and maintain a smooth operation even when faced with unexpected inputs.

##### `__pycache__`

The `__pycache__` directory is a standard feature of Python that plays an important role in optimizing the execution of Python scripts. Here’s how it is relevant to this project:

1. **Performance Optimization**: When Python scripts are executed, the interpreter compiles the source code into bytecode, which is a more efficient representation for execution. The compiled bytecode is stored in the `__pycache__` directory, reducing the need for recompilation in subsequent runs of the same script. This enhances the performance of the application, especially during development and testing.

2. **File Structure**: The `__pycache__` directory contains files named according to the format:
    `<module_name>.cpython-<version>.pyc`
    This naming convention includes the module name and the version of the Python interpreter used to create the bytecode, ensuring compatibility across different versions.

3. **Automatic Management**: Python automatically manages the `__pycache__` directory, creating and updating it as needed. Developers typically do not need to interact with this directory directly, and it can be safely ignored in version control systems by including it in a `.gitignore` file.


## Usage

**`upd_server.py`** effectively demonstrates the core functionalities of the lock server and client interactions. It provides a practical example of how the locking mechanism can be utilized in a distributed environment, showcasing the ability to manage concurrent access to shared resources. 

In this file 5 clients are performing the following steps:

1. *Initialization:* Each client creates an instance of Distributed_Client with a unique client_id (e.g., client_1, client_2, etc.), which helps identify the client in logs and output.

2. *File Mapping:* Each client is mapped to a specific file based on its ID:

    - Clients 1 and 3: Write to file_0
    - Client 2: Writes to file_2
    - Clients 4 and 5: Write to file_3

3. *Acquire Lock:* Each client attempts to acquire a lock on its designated file to ensure exclusive access while writing. The lock mechanism prevents multiple clients from writing to the same file simultaneously, avoiding race conditions and potential data corruption.

4. *Write to File:* Once a client successfully acquires the lock:

    It writes a log entry to its designated file. This log entry includes the client_id and specifies which file it’s writing to (e.g., "Log entry from client_1 writing to file_0").
    This step guarantees that each client writes only once to the designated file, and each log entry includes the client ID and target file for clear traceability.

5. *Release Lock:* After writing, the client releases the lock, allowing other clients to access the file if needed. Releasing the lock is done in a finally block to ensure the lock is always freed, even if an error occurs during writing.

6. *Close Connection:* Each client closes its connection to complete the process.

## Dependencies
This project requires Python 3.12.3. No additional external libraries are necessary, as it relies solely on Python's standard libraries.

## Environment
This code was developed and tested on Ubuntu 24.04.1 LTS.

## Authors
Maria Gragera Garces