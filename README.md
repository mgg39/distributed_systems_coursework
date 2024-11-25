This project was developped as part of the [University of Edinburgh's 2024 Distributed System's course](http://www.drps.ed.ac.uk/20-21/dpt/cxinfr11022.htm) given by (Yuvraj Patel)[https://homepages.inf.ed.ac.uk/ypatel/]. I took this course during my 1st year of PhD studies as a means of exploring the classical world of distributed computing. The objective of this coursework was to build our first simulated distributed system made up of various servers. My proposal although incomplete served as a good learning experience and could easly be extended into a fully solid and rigid proposal. If you would like to see the coursework instructions please see the 'Instructions.md' file.

### Design Document for Distributed Lock Manager

1. Overview
-----------
The Distributed Lock Manager ensures synchronization across multiple servers in a distributed system.
- Objective: Enable synchronized file operations with fault-tolerant leader election and lock state replication.
- Key Features:
  - Leader election using server IDs (lowest ID becomes leader).
  - Lock queueing for fair acquisition.
  - State replication to followers.
  - Lease-based locks with expiration handling.

2. System Architecture
-----------------------
- Leader Server: Manages locks, processes client requests, and propagates updates to followers.
- Follower Servers: Synchronize state with the leader and participate in leader election.
- Clients: Send requests to acquire/release locks and perform file operations.

System Diagram:
  Client 1   Client 2   Client N
     |          |           |
     +----------+-----------+
                |
         Leader Server (Server 1)
                |
     ---------------------------
     |                         |
Follower Server 2       Follower Server 3

3. Workflow
-----------
Lock Acquisition:
- Client sends `acquire_lock` to leader.
- Leader grants lock or queues the request.
- Leader propagates lock state to followers.

File Operation:
- Client with lock sends `append_file` to leader.
- Leader updates the file and propagates changes to followers.

Lock Release:
- Client sends `release_lock`.
- Leader releases lock and grants it to the next client in the queue.

Leader Failure:
- Followers detect failure via heartbeat timeout.
- A new leader is elected (lowest ID active server).

Workflow Sequence:
  Client --> Leader --> Followers --> Client

4. Assumptions
--------------
- Leader is always the lowest ID active server.
- Clients communicate only with the leader.
- Network latency is consistent and minimal.
- Locks are leased for a fixed duration (default: 20 seconds).

5. Key Scenarios
-----------------
Normal Operation:
- Clients acquire and release locks successfully.
- File operations are synchronized.

Leader Failure:
- Followers elect a new leader and resume operation.

Lock Expiry:
- Expired locks are released, and the next client in the queue gets the lock.

6. Limitations
--------------
- Assumes clients know server addresses.
- Limited fault tolerance for simultaneous failures.
- Network delays may cause temporary inconsistencies.

7. Key Components in Code
--------------------------
Server:
- `acquire_lock`: Handles lock requests and queues clients.
- `release_lock`: Releases locks and processes the queue.
- `notify_followers_*`: Updates follower states for lock or file changes.

Client:
- `acquire_lock`: Manages retries with exponential backoff.
- `release_lock`: Ensures locks are released post-operation.
- `find_leader`: Identifies the current leader server.

## Dependencies
This project requires Python 3.12.3. No additional external libraries are necessary, as it relies solely on Python's standard libraries.

## Environment
This code was developed and tested on Ubuntu 24.04.1 LTS.

## Authors
Maria Gragera Garces
