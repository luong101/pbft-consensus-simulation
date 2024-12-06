package mainpBFT

import (
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
)

type pbftCore struct {
	n        uint // Số Replica
	f        uint // Số Byzantine mà có thể tolerate
	sequence uint
	view     uint
	nodes    []peer.ID // Cho TH chọn primary bằng ID nhỏ nhất
}

// Tính số lượng Byzantine tối đa của hệ thống
func calcByzantineTolerance(n uint) uint {

	if n <= 1 {
		return 0
	}

	f := (n - 1) / 3
	return f
}

func newPbftCore(total uint) pbftCore {

	return pbftCore{
		sequence: 0,
		view:     0,
		n:        total,
		f:        calcByzantineTolerance(total),
	}
}

// TÌM TRÊN VIEW, ĐẢM BẢO LUÂN PHIÊN
// Dựa trên view v, hàm xác định node chính (primary node) bằng công thức v%n
func (c pbftCore) primary(v uint) uint {
	return v % c.n
}

// return the index of the expected primary for the current view.
func (c pbftCore) currentPrimary() uint {
	return c.view % c.n
}



// TÌM TRÊN ID NHỎ NHẤT
func (c pbftCore) primaryNode() peer.ID {
	// Sao chép danh sách nodes để không làm thay đổi danh sách gốc.

	nodesCopy := make([]peer.ID, len(c.nodes))
	copy(nodesCopy, c.nodes)

	// Sắp xếp theo thứ tự tăng dần.
	sort.Slice(nodesCopy, func(i, j int) bool {
		return nodesCopy[i] < nodesCopy[j]
	})

	// Node đầu tiên trong danh sách sắp xếp là node chính.
	return nodesCopy[0]
}

func (c pbftCore) currentPrimaryNode() peer.ID {
	return c.primaryNode()
}

// Số lượng Prepare cần thiết để đồng thuận
func (c pbftCore) prepareQuorum() uint {
	return 2 * c.f
}

// Số lượng Commit cần thiết để đồng thuận
func (c pbftCore) commitQuorum() uint {
	return 2*c.f + 1
}

// Số lượng kết quả mà client cần nhận trước khi coi kết quả là hợp lệ
// f + 1
func MinClusterResults(n uint) uint {
	return calcByzantineTolerance(n) + 1
}

// Message ID
type messageID struct {
	view     uint
	sequence uint
}

func getMessageID(view uint, sequenceNo uint) messageID {
	return messageID{
		view:     view,
		sequence: sequenceNo,
	}
}
