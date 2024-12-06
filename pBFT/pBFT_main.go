package mainpBFT

import (
	"fmt"

	"github.com/blocklessnetwork/b7s/host" // Your host package
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/rs/zerolog"
)

func MainpBFT() {
	// Logger for debugging
	log := zerolog.New(zerolog.NewConsoleWriter())

	// Example address and port for the hosts
	address := "127.0.0.1"
	port := uint(8000)

	// Create Host
	host, err := host.New(log, address, port)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create host")
	}

	// List of peers (host IDs)
	peers := []peer.ID{
		host.ID(),
	}

	// Cluster ID for replicas
	clusterID := "example-cluster"

	// Create replicas for host
	replica, err := NewReplica(log, host, peers, clusterID)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create replica")
	}

	//Output the replica IDs
	fmt.Println("Replica 1 ID:", replica.IsPrimary())
	// Optionally start consensus logic or other operations here
	log.Info().Msg("Created 1 replicas successfully.")
	defer replica.Shutdown()
	replica.setPBFTMessageHandler()
	replica.
}

// package main

// import (
// 	mainpBFT "RAFT/pBFT"
// 	"fmt"

// 	"github.com/blocklessnetwork/b7s/host" // Your host package
// 	"github.com/libp2p/go-libp2p/core/peer"
// 	"github.com/rs/zerolog"
// )

// // Replica represents a replica in the system

// func main() {
// 	// Logger for debugging
// 	log := zerolog.New(zerolog.NewConsoleWriter())

// 	// Example address and port for the hosts
// 	address := "127.0.0.1"
// 	port := uint(8000)

// 	// Create 4 Hosts
// 	host1, err := host.New(log, address, port)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to create host1")
// 	}
// 	host2, err := host.New(log, address, port+1)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to create host2")
// 	}
// 	host3, err := host.New(log, address, port+2)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to create host3")
// 	}
// 	host4, err := host.New(log, address, port+3)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to create host4")
// 	}

// 	// List of peers (host IDs)
// 	peers := []peer.ID{
// 		host1.ID(),
// 		host2.ID(),
// 		host3.ID(),
// 		host4.ID(),
// 	}

// 	// Cluster ID for replicas
// 	clusterID := "example-cluster"

// 	// Create 4 replicas for each host
// 	replica1, err := mainpBFT.NewReplica(log, host1, peers, clusterID)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to create replica1")
// 	}
// 	replica2, err := mainpBFT.NewReplica(log, host2, peers, clusterID)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to create replica2")
// 	}
// 	replica3, err := mainpBFT.NewReplica(log, host3, peers, clusterID)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to create replica3")
// 	}
// 	replica4, err := mainpBFT.NewReplica(log, host4, peers, clusterID)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to create replica4")
// 	}

// 	//Output the replica IDs
// 	fmt.Println("Replica 1 ID:", replica1.IsPrimary())
// 	fmt.Println("Replica 2 ID:", replica2.IsPrimary())
// 	fmt.Println("Replica 3 ID:", replica3.IsPrimary())
// 	fmt.Println("Replica 4 ID:", replica4.IsPrimary())

// 	// Optionally start consensus logic or other operations here
// 	log.Info().Msg("Created 4 replicas successfully.")
// 	replica1.
// }
