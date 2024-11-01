# Distributed Lock Server

This coursework implements a UDP-based lock server for a distributed system as part of the Distributed Systems course at the University of Edinburgh. The server allows multiple clients to safely acquire and release locks on shared resources, demonstrating key concepts of concurrency and fault tolerance.

## Table of Contents

- [Overview](#overview)
- [File Structure](#file-structure)
- [Implementation Details](#implementation-details)
  - [RPC Initialization and Lock Management](#rpc-initialization-and-lock-management)
  - [Socket Programming and Multithreading](#socket-programming-and-multithreading)
  - [Crash Handling](#crash-handling)
- [Usage](#usage)
- [Dependencies](#dependencies)
- [Environment](#environment)
- [Authors](#authors)

## Overview

The system consists of a lock manager server and multiple distributed clients that communicate with the server to manage access to shared files. Clients must acquire a lock before they can append data to a file, ensuring that only one client can modify a file at a time.

## File Structure

The project contains the following folders:

1. Main:
This folders contains the main versions of the code used to build this task:
    1.1. **`upd_server.py`**: Implements the UDP lock server.
    1.2. **`rpc_connection.py`**: Handles RPC connections between clients and the server.
    1.3. **`distributed_client.py`**: Represents the client that interacts with the server to manage locks and append data to files.
    1.4. **`test_client.py`**: Contains a testing framework to simulate multiple clients attempting to acquire locks and write to files.

2. Test_crashes: TODO: fill this
This folders contains the test performed on the network to ensure fault tolerance:
    2.1
    2.2
    2.3

## Implementation Details

### RPC Initialization and Lock Management

- **RPC Connection**: The `RPCConnection` class in `rpc_connection.py` manages UDP communication with the server. It implements a timeout mechanism for request handling.
- **Lock Management**: The server supports three main operations for lock management:
  - `acquire_lock`: Clients request locks and receive a lease duration if granted.
  - `release_lock`: Clients release locks they hold.
  - `append_file`: Clients append data to files only if they hold the lock.

### Socket Programming and Multithreading

- The server is implemented using Python’s `socket` library for UDP communication. It listens for client requests in a loop, spawning new threads to handle each request, allowing multiple clients to interact with the server simultaneously.
- Multithreading is used for both the server and client implementations. The server runs a background thread to monitor lock expiration, while clients run threads to send heartbeat messages without blocking their main operations.

### Crash Handling
TODO: improve details here

## Network Failure
- **Lock Expiration**: The server uses a lease mechanism for locks, where a lock is automatically released if not renewed within a specified duration (`LOCK_LEASE_DURATION`). This prevents locks from being held indefinitely due to client crashes.

## Client crash
- **Heartbeat Mechanism**: Clients send periodic heartbeat messages to renew their locks. If a client fails to send a heartbeat before the lease expires, the server automatically releases the lock.

## Server crash
- **State Persistence**: The server's state is saved to a JSON file (`server_state.json`) on startup and shutdown, allowing it to recover its state after crashes.


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