package main

import (
	node "RAFT/pkg/node"
	"context"
	"fmt"
	"log"
	"math"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

type DiscoverHandler struct {
	Node *node.Node
}

func (dh *DiscoverHandler) HandlePeerFound(pi peer.AddrInfo) {
	fmt.Printf("[mDNS] Discovered peer: %s\n", pi.ID)

	// Attempt to connect to the discovered peer
	if err := dh.Node.Host.Connect(context.Background(), pi); err != nil {
		fmt.Printf("Failed to connect to peer %s: %v\n", pi.ID, err)
		return
	}
	
	// Send a Hello message
	// dh.Node.SendHello(pi.ID)
	dh.Node.Host.Peerstore().AddAddrs(pi.ID, pi.Addrs, math.MaxInt64)
	dh.Node.NumberOfNode = len(dh.Node.Host.Peerstore().Peers())
	dh.Node.Start()
}

func main() {
	// Create a new Node
	node, err := node.NewNode()
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Host.Close()

	fmt.Printf("Node created. ID: %s \n", node.Host.ID().ShortString())

	// Register RPC services
	err = node.RegisterServices()
	if err != nil {
		log.Fatalf("Failed to register RPC services: %v", err)
	}

	// Set up mDNS discovery
	serviceName := "p2p-mdns"
	mdnsService := mdns.NewMdnsService(node.Host, serviceName, &DiscoverHandler{Node: node})
	if err := mdnsService.Start(); err != nil {
		log.Fatalf("Failed to start mDNS discovery: %v", err)
	}
	defer mdnsService.Close()

	fmt.Println("[mDNS] Service started, waiting for peers...")

	// Keep the program running
	select {}
}
