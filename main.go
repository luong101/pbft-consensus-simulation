package main

import (
	"context"
	"fmt"
	"log"

	"github.com/libp2p/go-libp2p"
	gorpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

var protocolID = protocol.ID("/p2p/rpc/ping")

type HelloService struct{}

// HelloRequest is the structure for a Hello message
type HelloRequest struct {
	Message string
	Sender  string
}

// HelloResponse is the structure for the response
type HelloResponse struct {
	Ack string
}

// SendHello handles incoming Hello messages
func (hs *HelloService) SendHello(ctx context.Context, req *HelloRequest, res *HelloResponse) error {
	log.Printf("Received message from %s: %s", req.Sender, req.Message)
	res.Ack = "Hello received by " + req.Sender
	return nil
}

// Notifee là một struct implement interface mdns.Notifee
type Notifee struct {
	host host.Host
	port int
}

func (n *Notifee) HandlePeerFound(peerInfo peer.AddrInfo) {
	// Add peer info to the peerstore
	n.host.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, peerstore.PermanentAddrTTL)

	// Connect to the peer
	err := n.host.Connect(context.Background(), peerInfo)
	if err != nil {
		log.Printf("Failed to connect to peer %s: %v", peerInfo.ID, err)
		return
	}
	log.Printf("Connected to peer: %s", peerInfo.ID.ShortString())

	// Create an RPC client for the discovered peer
	client := gorpc.NewClient(n.host, protocolID)

	// Send a Hello message to the peer
	req := &HelloRequest{
		Message: "Hello from " + n.host.ID().ShortString(),
		Sender:  n.host.ID().ShortString(),
	}
	res := &HelloResponse{}
	err = client.Call(peerInfo.ID, "HelloService", "SendHello", req, res)
	if err != nil {
		log.Printf("Failed to send Hello message to peer %s: %v", peerInfo.ID, err)
		return
	}
	log.Printf("Received response from peer %s: %s", peerInfo.ID, res.Ack)
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
	fmt.Println("Host created with ID:", host.ID().ShortString())
	for _, addr := range host.Addrs() {
		fmt.Println("Listening on:", addr.String())
	}

	// Create an RPC server and register the HelloService
	rpcServer := gorpc.NewServer(host, protocolID)
	helloService := &HelloService{}
	err = rpcServer.Register(helloService)
	if err != nil {
		log.Fatal("Failed to register HelloService:", err)
	}

	// Start mDNS discovery with Notifee
	notifee := &Notifee{host: host}
	serviceName := "local-discovery"
	mdnsService := mdns.NewMdnsService(host, serviceName, notifee)
	if err := mdnsService.Start(); err != nil {
		log.Fatal("Failed to start mDNS service:", err)
	}
	defer mdnsService.Close()

	fmt.Println("mDNS service started. Discovering peers...")

	select {} // Keep the application running
}
