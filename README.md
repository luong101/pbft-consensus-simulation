# pBFT Consensus Simulation

This repository contains an implementation of the **Practical Byzantine Fault Tolerance (pBFT)** consensus algorithm in Go. It simulates a distributed network of nodes reaching consensus, even in the presence of malicious (Byzantine) actors.


## Features

- **P2P Consensus Execution:** Simulates the core consensus and view-change flows using a peer-to-peer network cluster.
  - **Three-Phase Agreement Protocol:** Implements the normal-case consensus path with the **Pre-Prepare**, **Prepare**, and **Commit** phases.
  - **Liveness & Recovery Protocol:** Implements **View-Change** and **New-View** phases to safely transition the cluster to a new primary node if the current primary fails.
- **Single-Process Local Network:** The simulation runs entirely within a single Go process, utilizing [`libp2p`](https://libp2p.io/) to establish concurrent stream-based communication between in-memory replicas bound to local ports (`127.0.0.1:8000` to `8003`).
- **Byzantine Fault Tolerance:** Capable of tolerating up to $f = \lfloor \frac{n - 1}{3} \rfloor$ malicious nodes, where $n$ is the total number of nodes (default is 4 nodes, meaning it can tolerate $f = 1$ Byzantine node).
- **Interactive Simulation Setup:** On startup, the terminal sequentially prompts you to explicitly configure each replica node as honest (`0`) or Byzantine (`1`) to see how the system handles faults.

## Limitations & Simplifications
As this is an educational simulation designed to demonstrate the state machine transitions of pBFT, several components have been simplified:

- **No Cryptographic Signatures:** While the message structures contain signature fields, actual cryptographic signing and signature verification have been disabled/commented out for simplicity. The simulation relies on the honest behavior of non-Byzantine nodes rather than cryptographic proof of authenticity.
- **Internal Request Injection (No Network Client):** There is no external HTTP API or network client provided for submitting dynamic requests. The orchestrator initiates a single hardcoded request (`payload: "hello"`, `id: "1"`) on startup. It bypasses network broadcast and directly injects this request into the in-memory replicas by looping over them and calling their internal request handlers.
- **Bypassed Reply Phase:** In standard pBFT, each replica sends a cryptographic `Reply` directly to the client, and the client waits for $f+1$ identical replies to accept the result. In this simulation, the "Reply" phase is bypassed: the cluster executes requests internally by writing them directly to local `.txt` ledgers, without returning active consensus proofs to an external client.
- **No Checkpointing:** The pBFT checkpointing phase is not implemented. The protocol assumes a lower sequence number bound of `0` during view changes.
- **Basic Ledger Storage:** Instead of a complex blockchain, each node simply maintains a local text-based ledger (`chainFile_<PeerID>.txt`). These are stored as simple CSV files with the exact format: `PreviousBlockHash,BlockHeight,BlockHash`.

## Repository Structure

- `main.go`: The entry point of the simulation. It simply calls the `pbft.MainpBFT()` function. *(Note: The project imports this as `RAFT/pBFTCode` because the Go module is named `RAFT`, despite this being a pBFT protocol).*
- `go.mod` / `go.sum`: Go module files declaring `module RAFT` and listing dependencies (e.g., `go-libp2p`, `zerolog`, `b7s`).
- `pBFTCode/`: Contains the complete pBFT state machine logic.
  - `pBFT_main.go`: The main orchestrator. It spawns 4 replica hosts, guides the interactive Byzantine setup, connects the P2P network, and injects the first mock request.
  - `pBFT_host.go`: Contains bootstrapping helper functions to interconnect all replica `libp2p` hosts.
  - `replica.go`: Main cluster element wrapping core states, receiving streams, and demuxing P2P messages to the appropriate phase handlers.
  - `core.go`: Math core. Calculates Byzantine tolerance ($f$), quorum requirements ($2f$ prepares, $2f+1$ commits), and determines the primary replica for the current view ($v \pmod n$).
  - `conditions.go`: Implements validation conditions for quorum checks (`prepared`, `committed`, `viewChangeReady`).
  - `preprepare.go`: Implements the Pre-Prepare phase logic (broadcasted strictly by the primary replica).
  - `prepare.go`: Implements the Prepare phase messaging and validation.
  - `commit.go`: Implements the Commit phase broadcast and receiving logic.
  - `view_change.go`: Implements the View-Change broadcasting, validation, and recovery pipeline.
  - `new_view.go`: Handles the New-View logic when a new primary takes over after a view change.
  - `execute.go`: Executes state transitions. Appends the committed block to the CSV ledger and stops/restarts request timers.
  - `request.go`: Entry point for client requests. Checks for duplicates, starts request timers, and forwards the transaction to Pre-Prepare if the receiving node is the primary.
  - `block.go`: Defines basic `Block` / `Blockchain` structures and provides functions to initialize and write CSV files.
  - `message.go`: Defines the Go structures for all pBFT message types.
  - `receipts.go`: Synchronized maps holding peer receipts for Prepare, Commit, and ViewChange.
  - `state.go`: Memory layout for the replica (state locks, cached requests, execution records).
  - `timer.go`: Request inactivity timer that triggers a view change if the primary does not make progress.
  - `messaging.go`: Concurrent broadcast and direct P2P stream messaging using the `libp2p` host.
  - `serialization.go`: Custom JSON marshallers enabling stable encoding/decoding of structs and Go types like `peer.ID`.
  - `digest.go`: Helper generating SHA256 hashes of serialized JSON structures.
  - `params.go`: Constants and error definitions (protocol ID, timeouts, default bounds).
  - `outstanding.go`: Logic to identify pending requests that have been seen but not yet executed or pipeline-queued.
  - `tracing.go`: Telemetry instrumentation helper.

## Prerequisites

- **Go:** Version 1.23.3 or higher.

## How to Run

1. Clone the repository and navigate to the `pBFT` directory:
   ```bash
   cd pBFT
   ```

2. Download the dependencies:
   ```bash
   go mod tidy
   ```

3. Run the simulation. **The entire cluster will run inside this single terminal window**:
   ```bash
   go run main.go
   ```

4. **Interactive Setup:** The simulation will start 4 replicas. For each replica, you will be prompted sequentially to enter whether it should act as a Byzantine node:
   ```
   Is this node Byzantine (0: No | 1: Yes): 
   ```
   *Enter `0` for an honest node and `1` for a Byzantine node. (For a 4-node cluster, keep Byzantine nodes to 1 or 0 to allow consensus to be reached).*

5. **Observe:** The internal nodes will communicate via `libp2p` over local ports, logging their state transitions (`PrePrepare` -> `Prepare` -> `Commit`). The cluster will process the single injected request ("hello") and then wait indefinitely.

6. **Check Ledgers:** After the request is processed, check the generated `chainFile_<PeerID>.txt` files in the directory to see the newly appended blocks in CSV format.

## Dependencies

- [libp2p/go-libp2p](https://github.com/libp2p/go-libp2p): A modular network stack for peer-to-peer applications.
- [rs/zerolog](https://github.com/rs/zerolog): A fast and simple JSON logger for Go.
- [blocklessnetwork/b7s](https://github.com/blocklessnetwork/b7s): Used for some underlying node definitions, telemetry, and consensus data structures.
