package mainpBFT

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Định nghĩa các cấu trúc và hàm để lưu trữ và quản lý các message

// Ánh xạ từ node ID (peer.ID) tới Prepare message
type prepareReceipts struct {
	m map[peer.ID]Prepare
	*sync.Mutex
}

func newPrepareReceipts() *prepareReceipts {

	pr := prepareReceipts{
		m:     make(map[peer.ID]Prepare),
		Mutex: &sync.Mutex{},
	}

	return &pr
}

// Ánh xạ từ node ID (peer.ID) tới Commit message
type commitReceipts struct {
	m map[peer.ID]Commit
	*sync.Mutex
}

func newCommitReceipts() *commitReceipts {

	cr := commitReceipts{
		m:     make(map[peer.ID]Commit),
		Mutex: &sync.Mutex{},
	}

	return &cr
}

// Ánh xạ từ node ID (peer.ID) tới View change

// type PrepareInfo struct {
// 	View           uint                `json:"view"`
// 	SequenceNumber uint                `json:"sequence_number"`
// 	Digest         string              `json:"digest"`
// 	PrePrepare     PrePrepare          `json:"preprepare"`
// 	Prepares       map[peer.ID]Prepare `json:"prepares"`
// }

// type ViewChange struct {
// 	View     uint
// 	Prepares []PrepareInfo

// 	// Signed digest of the view change message.
// 	Signature string

// 	// Technically, view change message also includes:
// 	//	- n - sequence number of the last stable checkpoint => not needed here since we don't support checkpoints
// 	//  - C - 2f+1 checkpoint messages proving the correctness of s => see above
// }

type viewChangeReceipts struct {
	m map[peer.ID]ViewChange
	*sync.Mutex
}

func newViewChangeReceipts() *viewChangeReceipts {

	vcr := viewChangeReceipts{
		m:     make(map[peer.ID]ViewChange),
		Mutex: &sync.Mutex{},
	}

	return &vcr
}
