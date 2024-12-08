package mainpBFT

import (
	"context"
	"fmt"

	"github.com/blocklessnetwork/b7s/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func connectAllHost(pbft_Host []*host.Host) {
	for _, cur_host := range pbft_Host {
		for _, target := range pbft_Host {
			if cur_host.ID() != target.ID() {
				target_addr := fmt.Sprintf("%s/p2p/%s", target.Addrs()[0], target.ID())
				target_info, err := peer.AddrInfoFromString(target_addr)
				if err != nil {
					fmt.Printf("Failed to convert", err)
				}
				err = cur_host.Connect(context.Background(), *target_info)
				if err != nil {
					fmt.Printf("Failed to connect", err)
				}
			}
		}
	}

	// for _, cur_host := range pbft_Host {
	// 	connectedPeers := cur_host.Network().Peers()
	// 	if len(connectedPeers) == 0 {
	// 		fmt.Println("No peers connected.")
	// 		return
	// 	}
	// 	fmt.Println("Connected peers:")
	// 	for _, peerID := range connectedPeers {
	// 		fmt.Printf("- %s\n", peerID)
	// 	}
	// }

}
