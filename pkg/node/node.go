package node

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	protocolID  = protocol.ID("/p2p/rpc/ping")
	serviceName = "RaftService"
	RequestVote = "RequestVote"
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
}

type RequestVoteReply struct {
	TermID      int
	VoteGranted bool
}

type Node struct {
	//private
	electionTimeout time.Duration
	votedFor        *peer.ID
	termID          int
	heartBeatCh     chan (int)
	lastLogIndex    int
	lastLogTerm     int
	//public
	State        Role
	Host         host.Host
	NumberOfNode int
	RPCServer    *rpc.Server
	RPCClient    *rpc.Client
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
		electionTimeout: time.Duration(150+time.Now().UnixNano()%150) * time.Millisecond,
		termID:          0,                 // Start termID from 0
		votedFor:        nil,               // No vote initially
		heartBeatCh:     make(chan int, 1), // Buffered channel for heartbeats
		lastLogIndex:    0,                 // Placeholder for last log index
		lastLogTerm:     0,                 // Placeholder for last log term
	}

	return node, nil
}

// RegisterServices registers the RPC services to the server
func (node *Node) RegisterServices() error {
	return node.RPCServer.Register(&RaftService{node})
}

func CopyToIfaces(in []*RequestVoteReply) []interface{} {
	ifaces := make([]interface{}, len(in))
	for i := range in {
		in[i] = &RequestVoteReply{}
		ifaces[i] = in[i]
	}
	return ifaces
}

func Ctxts(n int) []context.Context {
	ctxs := make([]context.Context, n)
	for i := 0; i < n; i++ {
		ctxs[i] = context.Background()
	}
	return ctxs
}

func (rft *RaftService) RequestVote(ctx context.Context, request *RequestVoteReq, reply *RequestVoteReply) error {
	node := rft.node
	if request.TermID < node.termID {
		reply.TermID = node.termID
		reply.VoteGranted = true
		return nil
	}

	if request.TermID > node.termID {
		node.termID = request.TermID
		node.votedFor = nil // reset vote
	}

	if node.votedFor == nil || *node.votedFor == request.CandidateID {
		if request.LastLogTerm > node.lastLogTerm || (request.LastLogTerm == node.lastLogTerm && request.LastLogIndex >= node.lastLogIndex) {
			node.votedFor = &request.CandidateID
			reply.TermID = node.termID
			reply.VoteGranted = true
			return nil
		}
	}

	reply.TermID = node.termID
	reply.VoteGranted = false
	return nil
}

func (node *Node) BroadCastRequestVote() {
	fmt.Println("[!] ", node.Host.ID(), " broad cast")
	peers := node.Host.Peerstore().Peers()
	var reply RequestVoteReply
	var numberOfVote int = 1
	
	for _, peer := range peers {
		err := node.RPCClient.Call(peer, serviceName, RequestVote, &RequestVoteReq{
			TermID:       node.termID,
			CandidateID:  node.Host.ID(),
			LastLogIndex: node.lastLogIndex,
			LastLogTerm:  node.lastLogTerm,
		}, &reply)

		if err != nil {
			fmt.Println("Error at", peer.ShortString())
		} else {
			if reply.VoteGranted {
				numberOfVote++
			}
			fmt.Println("Vote", peer.ShortString(), reply.VoteGranted)
		}
	}

	if numberOfVote > node.NumberOfNode/2 {
		node.State = 2
	}
	// var replies = make([]*RequestVoteReply, len(peers))
	// node.termID++

	// errs := node.RPCClient.MultiCall(
	// 	Ctxts(len(peers)),
	// 	peers,
	// 	serviceName,
	// 	RequestVote,
	// 	RequestVoteReq{
	// 		TermID:       node.termID,
	// 		CandidateID:  node.Host.ID(),
	// 		LastLogIndex: node.lastLogIndex,
	// 		LastLogTerm:  node.lastLogTerm,
	// 	},
	// 	CopyToIfaces(replies),
	// )

	// for i, err := range errs {
	// 	if err != nil {
	// 		fmt.Println("[!] error at", i, err.Error())
	// 	} else {
	// 		fmt.Println("Node", i, "vote", replies[i].VoteGranted)
	// 	}
	// }
}

func (node *Node) Start() {
	for {
		switch node.State {
		case Follower:
			<-time.After(node.electionTimeout)
			node.State = Candidate
		case Candidate:
			node.BroadCastRequestVote()
			time.Sleep(10 * time.Second)
		case Leader:
			fmt.Println("I am a leader")
			time.Sleep(5 * time.Second)
		}
	}
}
