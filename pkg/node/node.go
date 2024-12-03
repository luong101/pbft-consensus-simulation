package node

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	Follower Role = iota
	Candidate
	Leader
)

type Role int

type RaftScv struct {
	Node *Node
}

type RequestVoteReq struct {
	TermID       int
	CandidateID  peer.ID
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	TermID      int
	VoteGranted bool
}

type Entries struct {
	ID  int
	Msg string
}

type AppendEntriesReq struct {
	TermID       int
	LeaderID     peer.AddrInfo
	PrevLogTerm  int
	Entry        []Entries
	LeaderCommit int
}

type AppendEntriesReply struct {
	TermID  int
	Success bool
}

type Node struct {
	// Private
	electionTimeout time.Duration
	termID          int
	votedFor        *peer.ID
	peerInfo        []peer.AddrInfo
	heartBeatCh     chan (int)
	lastLogIndex    int
	lastLogTerm     int
	// Public
	State        Role
	NodeID       peer.ID
	NumberOfNode int
	Host         host.Host
	RPCServer    *gorpc.Server
	RPCClient    *gorpc.Client
}

// NewNode creates a new Raft node
func NewNode(nodeID peer.ID) (*Node, error) {
	// Initialize the node
	host, err := libp2p.New()
	if err != nil {
		return nil, err
	}
	node := &Node{
		NodeID:          nodeID,
		Host:            host,
		peerInfo:        nil,
		electionTimeout: time.Duration(150+time.Now().UnixNano()%150) * time.Millisecond,
		State:           Follower,
		termID:          0,
		votedFor:        nil,
		NumberOfNode:    0,
		heartBeatCh:     make(chan int, 1),
		lastLogIndex:    0,
		lastLogTerm:     0,
	}

	return node, nil
}

// RequestVoteRPC handles incoming RequestVote requests
func (rscv *RaftScv) RequestVoteRPC(ctx context.Context, req *RequestVoteReq, rep *RequestVoteReply) error {
	node := rscv.Node

	if req.TermID < node.termID {
		rep.TermID = node.termID
		rep.VoteGranted = false
		return fmt.Errorf("here 1")
	}

	// If the candidate's term is greater, update the term and reset vote
	if req.TermID > node.termID {
		node.termID = req.TermID
		node.votedFor = nil // Reset vote for the new term
	}

	// Check if the node has already voted for another candidate in the current term
	if node.votedFor == nil || *node.votedFor == req.CandidateID {
		// Log comparison: Candidate's log must be at least as up-to-date
		lastLogTerm, lastLogIndex := node.lastLogTerm, node.lastLogIndex // Implement this method
		if req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			node.votedFor = &req.CandidateID
			rep.TermID = node.termID
			rep.VoteGranted = true
			return fmt.Errorf("here true")

		}
	}

	// If conditions are not met, deny the vote
	rep.TermID = node.termID
	rep.VoteGranted = false
	return fmt.Errorf("Here 2")
}

func Ctxts(n int) []context.Context {
	ctxs := make([]context.Context, n)
	for i := 0; i < n; i++ {
		ctxs[i] = context.Background()
	}
	return ctxs
}

func CopyEnvelopesToIfaces(in []*RequestVoteReply) []interface{} {
	ifaces := make([]interface{}, len(in))
	for i := range in {
		in[i] = &RequestVoteReply{}
		ifaces[i] = in[i]
	}
	return ifaces
}

// StartElection starts the election process
func (node *Node) StartElection() error {
	node.State = Candidate
	node.termID++
	votes := 1 // vote for itself
	peers := node.Host.Peerstore().Peers()
	var replies = make([]*RequestVoteReply, len(peers))
	req := &RequestVoteReq{
		TermID:       node.termID,
		CandidateID:  node.NodeID,
		LastLogIndex: 0, // Placeholder
		LastLogTerm:  0, // Placeholder
	}

	errs := node.RPCClient.MultiCall(
		Ctxts(len(peers)),
		peers,
		"RaftScv",
		"RequestVoteRPC",
		req,
		CopyEnvelopesToIfaces(replies),
	)

	for i, err := range errs {
		if err != nil {
			fmt.Println("error ", i)
		} else {
			if replies[i].VoteGranted {
				votes++
				if votes > node.NumberOfNode/2 {
					node.State = Leader
					go node.broadcastHeartbeat()
					return nil
				}
			}
		}
	}
	return nil
}

// AppendEntriesRPC handles AppendEntries RPC
func (rscv *RaftScv) AppendEntriesRPC(ctx context.Context, req *AppendEntriesReq, rep *AppendEntriesReply) error {
	node := rscv.Node

	if req.TermID < node.termID {
		rep.TermID = node.termID
		rep.Success = false
		return nil
	}

	// Update state to follower if leader is valid
	node.State = Follower
	node.termID = req.TermID
	rep.TermID = req.TermID
	rep.Success = true

	// Log entries handling (not implemented yet)
	return nil
}

// AppendPeers
func (node *Node) AddPeer(p peer.AddrInfo) {
	node.peerInfo = append(node.peerInfo, p)
	node.NumberOfNode = len(node.peerInfo) + 1
}

// Heart beat broadcast
func (node *Node) broadcastHeartbeat() {
	for _, p := range node.peerInfo {
		req := &AppendEntriesReq{
			TermID:   node.termID,
			LeaderID: node.Host.Peerstore().PeerInfo(node.Host.ID()),
		}
		rep := &AppendEntriesReply{}
		err := node.RPCClient.Call(p.ID, "RaftScv", "AppendEntriesRPC", req, rep)
		if err != nil {
			log.Printf("Failed to send heartbeat to peer %s: %v", p.ID, err)
		}
	}
}

// LogState
func (node *Node) LogState() {
	fmt.Println("[+] My role : ", node.State, " number of node in network ", node.NumberOfNode)
}

// Start the node
func (node *Node) Start() error {
	for {
		switch node.State {
		case Follower:
			select {
			case <-time.After(node.electionTimeout):
				node.State = 1
			}
		case Candidate:
			for {
				node.StartElection()
				time.Sleep(5 * time.Second) // Placeholder for candidate logic
			}
		case Leader:
			time.Sleep(50 * time.Millisecond) // Placeholder for leader logic
		}
	}
}
