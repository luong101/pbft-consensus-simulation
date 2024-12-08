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

func STTHost(r *Replica, id peer.ID) uint {
	list := peerIDList(r.peers)
	var i uint
	for i = 0; i < 4; i++ {
		if list[i] == id.String() {
			return i
		}
	}
	return 100
}

func MainpBFT() {

	// Logger for debugging
	replica_num := 4

	log := zerolog.New(zerolog.NewConsoleWriter())

	// Example address and port for the hosts
	address := "127.0.0.1"
	port := uint(8000)

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
		err = newBlockWithPrevBlockchain(replica.chainFile)
		if err != nil {
			fmt.Printf("Cannot create chain file from Replica %s", err)
		}
	}

	//Output the replica IDs
	for i := 0; i < replica_num; i++ {
		fmt.Println("Replica ", i+1, " Byzantine:", replicas[i].byzantine)
	}
	for i := 0; i < replica_num; i++ {
		fmt.Println("Replica ", i+1, " ID:", replicas[i].host.Addresses())
	}

	connectAllHost(pbft_Host)

	// Gửi Req
	req := "hello"
	// Tạo req
	request := Request{
		ID:        "1",
		Timestamp: time.Now(),
		Execute:   req,
	}
	// return replicas, request

	for i := 0; i < 4; i++ {
		ctx := context.Background()
		request.Origin = replicas[i].id
		replicas[i].processRequest(ctx, replicas[i].id, request)
	}

	// Close Host
	for i := 0; i < 4; i++ {
		defer replicas[i].Shutdown()
		defer pbft_Host[i].Close()
	}
	select {}

}
