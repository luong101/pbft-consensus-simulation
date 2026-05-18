# pBFT Consensus Simulation

A lightweight simulation of the Practical Byzantine Fault Tolerance (pBFT) protocol implemented in Go. This repository contains an educational simulation of pBFT where multiple local hosts (replicas) are created, connected, and run through the PBFT phases (pre-prepare → prepare → commit → view-change/new-view). The code is organized under `pBFT/pBFTCode`.

> Note: This project is intended as a simulation / learning tool, not production-ready consensus software.

## Quick summary

- Language: Go (module name: `RAFT`)
- Main entrypoint: `pBFT/main.go` which calls `pbft.MainpBFT()`
- The simulation creates a set of libp2p-based hosts, constructs `Replica` instances, and runs a simple request broadcast through the PBFT flow.
- The code lets you mark replicas as Byzantine (malicious) interactively at startup for experimenting with faulty behavior.

## Features

- Full local simulation of PBFT replicas and message handling (pre-prepare, prepare, commit, view-change, new-view).
- libp2p-based host and stream handling for replica-to-replica messaging.
- Simple per-replica chain file storage (each replica writes a chain file).
- Tracing and metrics hooks integrated (uses a tracing helper and metrics library).
- Interactive start: per-replica prompt to mark a node Byzantine or not.

## Project structure (high-level)

- pBFT/
  - main.go — small main that calls MainpBFT
  - go.mod, go.sum — module/dependency definitions
  - pBFTCode/
    - pBFT_main.go — creates hosts, builds replicas, prompts for Byzantine flags, broadcasts a request
    - replica.go — Replica struct, lifecycle, message handling registration, helper functions
    - message.go — definitions of PBFT message types (Request, PrePrepare, Prepare, Commit, ViewChange, NewView)
    - preprepare.go — sending/processing of PrePrepare messages
    - prepare.go — sending/processing of Prepare messages
    - commit.go — commit handling (commit.go in repo)
    - request.go — request handling (request.go in repo)
    - block.go — block and chain helpers (block.go in repo)
    - commit.go, receipts.go, outstanding.go, execute.go, digest.go, params.go, state.go, timer.go, tracing.go, view_change.go, new_view.go, serialization.go, messaging.go, pBFT_host.go, conditions.go — code implementing the PBFT core, message serialization, receipts, timers, view change, and supporting utilities.

(See repository for full file list with implementation details.)

## Requirements

- Go toolchain. `go.mod` declares:
  - module: `RAFT`
  - go: `1.23.2`
  - toolchain: `go1.23.3`
- The code depends on a number of third-party packages (libp2p, blockless b7s host, tracing packages, etc.). `go mod` will fetch them.

Note: The go.mod references newer Go versions (1.23.x). Use the go toolchain version compatible with the module, or a recent Go version that supports the used module features. If your local environment doesn't have that exact toolchain, you can generally run with a modern Go (>=1.20) but for best compatibility use the toolchain specified in go.mod.

## Build & run

From the repository root:

1. Download dependencies:
   - go mod download

2. Build (option A):
   - go build -o pbft-sim ./pBFT

   Or run directly (option B):
   - go run ./pBFT

3. Run:
   - ./pbft-sim
   - or: go run ./pBFT

Important runtime notes:
- The current simulation hardcodes `replica_num := 4` in `pBFT_main.go`. PBFT requires at least 4 replicas (3f+1 where f=1 here). You can change that value in code to experiment with different replica counts.
- The program prompts for each replica: `Is this node Byzantine (0: No | 1: Yes):` — enter `0` or `1` for each replica when asked.
- The simulation uses local loopback addresses and incremental ports (starting at port 8000). Ensure those ports are free.

## Typical behavior / what to expect

- The main routine creates N libp2p hosts, constructs a `Replica` for each host, prompts you whether each replica is Byzantine, connects the hosts, and sends a single test request `"hello"` (wrapped in a `Request`).
- Each Replica runs the PBFT message handlers and flows through pre-prepare → prepare → commit where appropriate.
- Each replica stores chain data in a file named `chainFile_<peerID>.txt` in the running directory (one file per replica).

## Configuration & common edits

- To change the number of replicas: edit `pBFT/pBFTCode/pBFT_main.go` and update `replica_num`.
- To change starting address/port: update `address` and `port` variables in `pBFT_main.go`.
- To automate Byzantine behavior instead of interactive prompting, modify the code that reads `fmt.Scanln(&isByzantine)` and provide a deterministic boolean per replica.

## Development notes & code walkthrough pointers

- Replica lifecycle: `NewReplica(...)` constructs a `Replica` with pbft core state, network host, tracer, and metrics. It registers the stream handler for the PBFT protocol and uses per-message serialization + handling.
- Message types and serialization: see `message.go` and `serialization.go`.
- PBFT phases implemented across files:
  - PrePrepare: `preprepare.go`
  - Prepare: `prepare.go`
  - Commit: `commit.go`
  - View change / NewView: `view_change.go`, `new_view.go`
- Networking: uses a host wrapper from `github.com/blocklessnetwork/b7s/host` and libp2p core APIs for peers, protocol IDs, and streams.

## Suggestions & improvements

- Add a non-interactive CLI (flags) to configure:
  - number of replicas
  - which replicas are Byzantine
  - request content and rate
  - logging/verbosity and output directory for chain files
- Add unit tests and small integration tests to automatically run the simulation with deterministic parameters.
- Add a LICENSE file if you intend to make the code reusable by others.
- Add descriptive comments in English for broader contributor understanding (the code contains helpful Vietnamese comments).

## Contributing

- Fork the repo, create a new branch, implement changes, then open a pull request.
- Add tests for new features where applicable.
- If you'd like, I can prepare PRs for:
  - adding a LICENSE (e.g., MIT)
  - creating a CLI to run simulations non-interactively
  - committing this README.md to the repository

## Troubleshooting

- If `go build` fails due to module or Go toolchain version mismatch, try:
  - installing a Go toolchain matching go.mod (if possible), or
  - use `GO111MODULE=on` and a modern `go` version (>= 1.20) and run `go mod tidy` then `go build`.
- If ports are in use, change the `port` starting value in `pBFT_main.go`.

## License

No license file is present in the repository. If you want a permissive license, I can add an `LICENSE` file (for example MIT) and commit it for you.

## Contact / author

If you want me to commit this README in the repository, or prepare additional documentation (command-line flags, automated scripts, CI), tell me and I will add/commit it.
