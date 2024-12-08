package node

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	protocolID    = protocol.ID("/p2p/rpc/ping")
	serviceName   = "RaftService"
	RequestVote   = "RequestVote"
	AppendEntries = "AppendEntriesRPC"
)

const (
	Follower Role = iota
	Candidate
	Leader
)

type Role int
type RaftService struct {
	node *Node
}
type RequestVoteReq struct {
	TermID       int
	CandidateID  peer.ID
	LastLogIndex int
	LastLogTerm  int
	NumberOfNode int
}

type RequestVoteReply struct {
	TermID      int
	VoteGranted bool
}

type Entries struct {
	ID  int    `json:"id"`
	Msg string `json:"msg"`
}
type AppendEntriesReq struct {
	TermID       int
	LeaderID     peer.ID
	prevLogTerm  int
	prevLogIndex int
	Entry        []Entries
	LeaderCommit int
}
type AppendEntriesReply struct {
	TermID  int
	Success bool
}

type Node struct {
	//private
	electionTimeout time.Duration
	votedFor        string
	termID          int
	heartBeatCh     chan (int)
	leaderCh        chan (int)
	lastLogIndex    int
	lastLogTerm     int
	commitIndex     int
	log             []Entries
	//public
	State        Role
	Host         host.Host
	NumberOfNode int
	RPCServer    *rpc.Server
	RPCClient    *rpc.Client
}

// Function to save committed logs to a JSON file
func saveLogsToJSON(logs []Entries, filename string) error {

	directory := ".\\commitedLog\\"
	path := directory + filename + ".json"

	// Ensure the directory exists
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		err := os.MkdirAll(directory, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	data, err := json.MarshalIndent(logs, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal logs to JSON: %w", err)
	}

	// Write JSON data to a file
	err = os.WriteFile(path, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write entries to file: %w", err)
	}

	return nil
}

// NewNode creates and initializes a Node with a libp2p host, RPC server, and RPC client
func NewNode() (*Node, error) {
	// Create a new libp2p host
	h, err := libp2p.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	// Create RPC server and client
	rpcServer := rpc.NewServer(h, protocolID)
	rpcClient := rpc.NewClientWithServer(h, protocolID, rpcServer)

	// Initialize the Node struct
	node := &Node{
		// Public fields
		Host:         h,
		RPCServer:    rpcServer,
		RPCClient:    rpcClient,
		State:        Follower, // Default state is Follower
		NumberOfNode: 0,

		// Private fields
		electionTimeout: time.Duration(15000+rand.Intn(15000)) * time.Millisecond,
		termID:          0,                 // Start termID from 0
		votedFor:        "",                // No vote initially
		heartBeatCh:     make(chan int, 1), // Buffered channel for heartbeats
		leaderCh:        make(chan int, 1),
		lastLogIndex:    0, // Placeholder for last log index
		lastLogTerm:     0, // Placeholder for last log term
		commitIndex:     0,
		log:             nil,
	}

	return node, nil
}

// RegisterServices registers the RPC services to the server
func (node *Node) RegisterServices() error {
	return node.RPCServer.Register(&RaftService{node})
}

func (rft *RaftService) RequestVote(ctx context.Context, request *RequestVoteReq, reply *RequestVoteReply) error {
	node := rft.node
	node.NumberOfNode = request.NumberOfNode
	// Reject request if the term is outdated
	if request.TermID <= node.termID {
		reply.TermID = node.termID
		reply.VoteGranted = false
		fmt.Println("This 1")
		return nil
	}

	// Update to a newer term if the request has a higher term
	if request.TermID > node.termID {
		node.termID = request.TermID
		node.votedFor = "" // Reset vote for the new term
	}

	if len(node.log) > 0 {
		node.lastLogIndex = len(node.log) - 1
		node.lastLogTerm = node.log[node.lastLogIndex].ID
	}

	// Check voting conditions
	if node.votedFor == "" || node.votedFor == request.CandidateID.ShortString() {
		if request.LastLogTerm > node.lastLogTerm ||
			(request.LastLogTerm == node.lastLogTerm && request.LastLogIndex >= node.lastLogIndex) {
			node.votedFor = request.CandidateID.ShortString()
			reply.TermID = node.termID
			reply.VoteGranted = true
			return nil
		}
	}

	// If conditions are not met, do not grant the vote
	reply.TermID = node.termID
	reply.VoteGranted = false
	return nil
}

func (rscv *RaftService) AppendEntriesRPC(ctx context.Context, req *AppendEntriesReq, rep *AppendEntriesReply) error {
	node := rscv.node
	node.heartBeatCh <- 1 // Signal a heartbeat received

	if req.TermID < node.termID {
		rep.TermID = node.termID
		rep.Success = false
		return nil
	}

	if req.prevLogTerm > 0 && (req.prevLogTerm >= len(node.log) || node.log[req.prevLogTerm].ID != req.prevLogTerm) {
		rep.TermID = node.termID
		rep.Success = false
		return nil
	}

	// Delete the existing entries conflict with the new one
	if req.prevLogIndex > 0 && req.prevLogIndex < len(node.log) && req.prevLogTerm != node.log[req.prevLogIndex].ID {
		node.log = node.log[:req.prevLogIndex]
	}

	//Append entry not already in log
	for _, entry := range req.Entry {
		if entry.ID > len(node.log) {
			node.log = append(node.log, entry)
		}
	}

	if len(node.log) > 0 {
		node.lastLogIndex = len(node.log) - 1
		node.lastLogTerm = node.log[node.lastLogIndex].ID
	}

	if req.LeaderCommit > node.commitIndex {
		node.commitIndex = min(req.LeaderCommit, len(node.log)-1)
		err := saveLogsToJSON(node.log, node.Host.ID().String())
		if err != nil {
			fmt.Println("Error saving logs:", err)
		} else {
			fmt.Println("Logs saved successfully to", node.Host.ID().ShortString())
		}

	}

	node.State = Follower
	node.termID = req.TermID
	rep.Success = true
	return nil
}
func (node *Node) sendHeartbeat(entries []Entries) {
	PrevLogIndex := 0
	PrevLogTerm := 0
	if len(entries) != 0 {
		if len(node.log) != 0 {
			PrevLogIndex = len(node.log) - 1
			PrevLogTerm = node.log[PrevLogIndex].ID
		}
		node.log = append(node.log, entries...)
	}

	for _, e := range node.log {
		fmt.Println("message: ", e.Msg)
	}

	for {
		if len(node.Host.Peerstore().Peers()) < 3 {
			os.Exit(1)
		}
		var countCommitedEntry int32 = 1
		// Ensure the node is still the leader
		if node.State != Leader {
			fmt.Println("[*] Node is no longer leader, stopping heartbeats.")
			return
		}

		fmt.Println("sendHeartbeat: Sending heartbeats to all peers...")
		peers := node.Host.Peerstore().Peers()

		var wg sync.WaitGroup
		var mu sync.Mutex // Protects access to shared resources
		responsivePeers := make([]peer.ID, 0)

		for _, p := range peers {
			// Skip self
			if p == node.Host.ID() {
				continue
			}

			wg.Add(1)
			// Launch a goroutine for each peer
			go func(peerID peer.ID) {
				defer wg.Done()
				// Create a timeout context for the heartbeat
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()

				var reply AppendEntriesReply

				err := node.RPCClient.CallContext(ctx, peerID, serviceName, AppendEntries, &AppendEntriesReq{
					TermID:       node.termID,
					LeaderID:     node.Host.ID(),
					prevLogTerm:  PrevLogTerm,
					prevLogIndex: PrevLogIndex,
					LeaderCommit: node.commitIndex,
					Entry:        node.log, // Heartbeat only
				}, &reply)

				if err != nil {
					fmt.Println("[!] Failed to send heartbeat to", peerID.ShortString(), ":", err)
					node.removeUnresponsiveNode(peerID)

				} else {
					fmt.Println("Heartbeat sent to", peerID.ShortString())
				}

				mu.Lock()
				if reply.Success {
					atomic.AddInt32(&countCommitedEntry, 1)
				}
				responsivePeers = append(responsivePeers, peerID)
				mu.Unlock()

			}(p)
		}

		wg.Wait()
		if countCommitedEntry > int32((len(responsivePeers)+1)/2) && node.commitIndex != len(node.log)-1 {
			node.commitIndex = len(node.log) - 1
			err := saveLogsToJSON(node.log, node.Host.ID().String())
			if err != nil {
				fmt.Println("Error saving logs:", err)
			} else {
				fmt.Println("Logs saved successfully to", node.Host.ID().ShortString())
			}
		}

		time.Sleep(5000 * time.Millisecond) // Periodic heartbeat
	}
}

func (node *Node) printEntries() {
	fmt.Println("Current term: ", node.termID)

	for _, e := range node.log {
		fmt.Println("command ", e.Msg)
	}
}

// removeUnresponsiveNode removes a node from the peer list
func (node *Node) removeUnresponsiveNode(peerID peer.ID) {
	// Remove peer from the Peerstore
	node.Host.Peerstore().RemovePeer(peerID)

	// Clear all addresses associated with the peer
	node.Host.Peerstore().ClearAddrs(peerID)

	// Log the removal
	fmt.Println("[*] Removed unresponsive node:", peerID.ShortString())
}

func (node *Node) BroadCastRequestVote() {
	fmt.Println("[!] ", node.Host.ID(), " broadcasting vote requests")
	peers := node.Host.Peerstore().Peers()
	var votes int32 = 1 // Vote for self (use atomic for thread safety)

	if len(node.Host.Peerstore().Peers()) < 5 && node.NumberOfNode == 0 {
		fmt.Println("[!] Insufficient peers to form a majority.")
		return
	}
	node.NumberOfNode = 1
	node.termID++                                // Increment term before election
	node.votedFor = node.Host.ID().ShortString() // Vote for self

	var LastLogIndex, LastLogTerm int
	if len(node.log) == 0 {
		LastLogIndex = 0
		LastLogTerm = 0
	} else {
		LastLogIndex = len(node.log) - 1
		LastLogTerm = node.log[LastLogIndex].ID
	}

	fmt.Println("Candidate current term: ", node.termID)

	var wg sync.WaitGroup
	var mu sync.Mutex // Protects access to shared resources
	responsivePeers := make([]peer.ID, 0)

	for _, p := range peers {
		if p == node.Host.ID() {
			continue
		}

		wg.Add(1)
		go func(peerID peer.ID) {
			defer wg.Done()

			// Create a timeout context for the vote request
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			var reply RequestVoteReply
			err := node.RPCClient.CallContext(ctx, peerID, serviceName, RequestVote, &RequestVoteReq{
				TermID:       node.termID,
				CandidateID:  node.Host.ID(),
				LastLogIndex: LastLogIndex,
				LastLogTerm:  LastLogTerm,
				NumberOfNode: node.NumberOfNode,
			}, &reply)

			if err != nil {
				fmt.Println("[!] Error requesting vote from", peerID.ShortString(), ":", err)
				node.removeUnresponsiveNode(peerID)
				return
			}

			if (node.Host.Peerstore().Peers().Len()) < 3 {
				os.Exit(1)
			}
			// Process the response
			mu.Lock()
			if reply.VoteGranted {
				atomic.AddInt32(&votes, 1)
			}
			responsivePeers = append(responsivePeers, peerID)
			mu.Unlock()

			fmt.Println("[+] Vote from", peerID.ShortString(), ":", reply.VoteGranted)
		}(p)
	}

	wg.Wait()

	// Check if the node has received majority votes
	if votes > int32((len(responsivePeers)+1)/2) {
		fmt.Println("[+] Received majority votes. Becoming leader.")
		node.leaderCh <- 1
	} else {
		fmt.Println("[-] Insufficient votes. Staying Candidate.")
	}

}

func (node *Node) Start() {
	for {
		select {
		case <-node.leaderCh:
			//do leader thing
			node.State = Leader
			fmt.Println("[+] Majority votes received. Becoming Leader.")

			command := "x = " + strconv.Itoa(node.termID)
			node.sendHeartbeat([]Entries{{ID: node.termID, Msg: command}})
		case <-node.heartBeatCh:
			//stay at follower
			node.State = Follower

			fmt.Println("[+] Receive heartbeat from leader.")
			node.printEntries()
		case <-time.After(node.electionTimeout):
			//change to candidate
			node.State = Candidate
			node.BroadCastRequestVote()
		}
	}
}
