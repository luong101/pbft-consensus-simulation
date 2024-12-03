package main

import (
	node "RAFT/pkg/node"
	"context"
	"fmt"
	"log"
	"math"

	"github.com/libp2p/go-libp2p"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

var protocolID = protocol.ID("/p2p/rpc/ping")

// Notifee handles peer discovery
type Notifee struct {
	host host.Host
	node *node.Node
}

// HandlePeerFound handles newly discovered peers
func (n *Notifee) HandlePeerFound(peerInfo peer.AddrInfo) {
	log.Printf("Discovered peer: %s", peerInfo.ID)
	// Add the peer info to the peerstore
	n.host.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, math.MaxInt64)
	log.Printf("Discovered peer: %s with addresses: %v", peerInfo.ID, peerInfo.Addrs)

	// Connect to the peer
	err := n.host.Connect(context.Background(), peerInfo)
	if err != nil {
		log.Printf("Failed to connect to peer %s: %v", peerInfo.ID, err)
		return
	}
	log.Printf("Connected to peer: %s", peerInfo.ID)

	// Add the peer to the node's peer list
	n.node.AddPeer(peerInfo)

	// Start leader election if this node is a follower
	if n.node.State == node.Follower {
		go n.node.StartElection()
	}
}

func main() {
	// Create a new libp2p host
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
	)
	if err != nil {
		log.Fatal("Failed to create libp2p host:", err)
	}
	defer host.Close()

	// Display host information
	fmt.Println("Host created with ID:", host.ID().String())
	for _, addr := range host.Addrs() {
		fmt.Println("Listening on:", addr.String())
	}

	// Create a new Node instance
	nodeInstance, err := node.NewNode(host.ID())
	if err != nil {
		log.Fatal("Failed to create node:", err)
	}
	nodeInstance.Host = host // Associate the host with the node

	//Register service
	raftService := &node.RaftScv{}
	nodeInstance.RPCServer = gorpc.NewServer(host, protocolID)
	err = nodeInstance.RPCServer.Register(raftService)
	if err != nil {
		log.Fatal("Failed to register RPC service:", err)
	}

	// Set up mDNS for peer discovery
	notifee := &Notifee{host: host, node: nodeInstance}
	serviceName := "raft-cluster"
	mdnsService := mdns.NewMdnsService(host, serviceName, notifee)
	if err := mdnsService.Start(); err != nil {
		log.Fatal("Failed to start mDNS service:", err)
	}
	defer mdnsService.Close()

	fmt.Println("mDNS service started. Discovering peers...")

	// Periodic logging of node state
	nodeInstance.Start()
	// go func() {
	// 	for {
	// 		time.Sleep(10 * time.Second)
	// 		nodeInstance.LogState()
	// 	}
	// }()

	select {} // Keep the application running
}
