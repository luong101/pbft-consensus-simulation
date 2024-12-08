package mainpBFT

import (
	// "context"
	"context"
	"fmt"
	"time"

	"github.com/blocklessnetwork/b7s/host" // Your host package
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/rs/zerolog"
)

var pbft_Host []*host.Host

func MainpBFT() {
	// Logger for debugging
	replica_num := 4

	log := zerolog.New(zerolog.NewConsoleWriter())

	// Example address and port for the hosts
	address := "127.0.0.1"
	port := uint(8000)

	// Create Host
	// pbft_Host := []*host.Host{}

	// List of peers (host IDs)
	peers := []peer.ID{}
	// Cluster ID for replicas
	clusterID := "example-cluster"

	// Create  host
	for i := 0; i < replica_num; i++ {
		subHost, err := host.New(log, address, port)

		pbft_Host = append(pbft_Host, subHost)

		if err != nil {
			log.Fatal().Err(err).Msg("Failed to create host")
		}

		peers = append(peers, subHost.ID())

		port++
	}

	// Create Replica for host
	replicas := []*Replica{}
	for _, cur_host := range pbft_Host {
		// Hỏi node có là Byzantine không
		isByzantine := false
		fmt.Println("Is this node Byzantine (0: No | 1: Yes): ")
		fmt.Scanln(&isByzantine)

		// Tạo Replica
		replica, err := NewReplica(log, cur_host, peers, clusterID, isByzantine)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to create replica")
		}
		replica.byzantine = isByzantine
		fmt.Println("This node is Byzantine: ", replica.byzantine)

		replicas = append(replicas, replica)

		// Tạo file lưu Blockchain
		// err = newBlockWithPrevBlockchain(replica.chainFile)
		// if err != nil {
		// 	fmt.Printf("Cannot create chain file from Replica %s", err)
		// }

		// Nhớ shutdown
		// defer replica.Shutdown()
	}

	//Output the replica IDs
	for i := 0; i < replica_num; i++ {
		fmt.Println("Replica ", i+1, " Byzantine:", replicas[i].byzantine)
	}
	for i := 0; i < replica_num; i++ {
		fmt.Println("Replica ", i+1, " ID:", replicas[i].host.Addresses())
	}

	connectAllHost(pbft_Host)
	// replicas[0].broadcast(context.Background(), "hello\n")

	// // Optionally start consensus logic or other operations here
	// connectAllHost(pbft_Host)
	// log.Info().Msg("Created 4 replicas successfully.")
	// for _, addr := range replica1.host.Addrs() {
	// 	fmt.Printf("Host Multiaddress: %s/p2p/%s\n", addr, replica1.host.ID())
	// }
	// connectAllHost(pbft_Host)

	// defer replica1.Shutdown()

	// replica1.broadcast(context.Background(), "hello")

	// Gửi Req
	// Tạo block mới
	req := "hello"
	// Tạo req
	for i := 0; i < 4; i++ {
		request := Request{
			ID:        "1",
			Timestamp: time.Now(),
			Origin:    replicas[i].id,
			Execute:   req,
		}
		ctx := context.Background()
		replicas[i].processRequest(ctx, replicas[i].id, request)
	}

	// Close Host
	for i := 0; i < 4; i++ {
		replicas[i].Shutdown()
		pbft_Host[i].Close()
	}
}
