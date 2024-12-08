package mainpBFT

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Prepare struct {
	View           uint
	SequenceNumber uint
	Digest         string // Block hash
	Signature      string // Ký digest prepare mess
	// Block          Block
}

// Gửi thông điệp Prepare sau khi nhận PrePrepare
func (r *Replica) sendPrepare(ctx context.Context, preprepare PrePrepare) error {

	// Nếu là Byzantine
	if r.byzantine {
		preprepare.View++
	}

	// Thông điệp của Prepare
	msg := Prepare{
		View:           preprepare.View,
		SequenceNumber: preprepare.SequenceNumber,
		Digest:         preprepare.Digest,
	}

	log := r.log.With().Str("digest", msg.Digest).Uint("view", msg.View).Uint("sequence_number", msg.SequenceNumber).Logger()
	log.Info().Msg("broadcasting prepare message")

	// Ký thông điệp
	// err := r.sign(&msg)
	// if err != nil {
	// 	return fmt.Errorf("could not sign prepare message: %w", err)
	// }

	// Broadcast message đến các node khác
	err := r.broadcast(ctx, &msg)
	if err != nil {
		return fmt.Errorf("could not broadcast prepare message: %w", err)
	}

	log.Info().Msg("prepare message successfully broadcast")

	// Record Prepare message
	r.recordPrepareReceipt(r.id, msg)

	return nil
}

// Record Prepare message từ 1 Replica
func (r *Replica) recordPrepareReceipt(replica peer.ID, prepare Prepare) {
	// Lấy msgID của Prepare
	msgID := getMessageID(prepare.View, prepare.SequenceNumber)

	// Nếu chưa có message nào có msgID thì lưu vào
	prepares, ok := r.prepares[msgID]
	if !ok {
		r.prepares[msgID] = newPrepareReceipts()
		prepares = r.prepares[msgID]
	}

	// Dùng lock để tránh xung đột khi ghi
	prepares.Lock()
	defer prepares.Unlock()

	// Bỏ qua nếu đã có message dc gửi trước đó
	_, exists := prepares.m[replica]
	if exists {
		r.log.Warn().Uint("view", prepare.View).Uint("sequence", prepare.SequenceNumber).Str("digest", prepare.Digest).Str("replica", replica.String()).Msg("ignoring duplicate prepare message")
		return
	}

	prepares.m[replica] = prepare // file state.go
}

// Xử lý Prepare message từ các bản sao và xác minh tính hợp lệ
func (r *Replica) processPrepare(ctx context.Context, replica peer.ID, prepare Prepare) error {
	log := r.log.With().Str("replica", replica.String()).Uint("view", prepare.View).Uint("sequence_no", prepare.SequenceNumber).Str("digest", prepare.Digest).Logger()

	log.Info().Msg("received prepare message")

	// Bỏ qua Prepare từ Primary node
	if replica == r.primaryReplicaID() {
		log.Warn().Msg("received prepare message from primary, ignoring")
		return nil
	}

	// Xác minh View
	if prepare.View != r.view {
		return fmt.Errorf("prepare has an invalid view value (received: %v, current: %v)", prepare.View, r.view)
	}

	// Xác minh chữ ký
	// err := r.verifySignature(&prepare, replica)
	// if err != nil {
	// 	return fmt.Errorf("could not verify signature for the prepare message: %w", err)
	// }

	// Record Prepare message
	r.recordPrepareReceipt(replica, prepare)

	// Chuyển qua giai đoạn Commit nếu đủ Prepare
	return r.maybeSendCommit(ctx, prepare.View, prepare.SequenceNumber, prepare.Digest)
}
