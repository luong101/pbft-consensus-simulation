package mainpBFT

import (
	"fmt"
	"context"

	"github.com/blocklessnetwork/b7s/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func connectAllHost(pbft_Host []*host.Host) {
	for _, cur_host := range pbft_Host {
		for _, target := range pbft_Host {
			if(cur_host != target){
				target_addr := fmt.Sprintf("%s/p2p/%s", target.Addrs()[0], target.ID())
				target_info, err := peer.AddrInfoFromString(target_addr)
				err = cur_host.Connect(context.Background(), *target_info)
				if err != nil {
					fmt.Printf("Failed to connect", err)
				}
			}
		}
	}
	
}
