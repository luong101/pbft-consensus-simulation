package mainpBFT

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Request struct {
	BaseMessage
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Origin    peer.ID   `json:"origin"`
	Execute   Block     `json:"execute"`
}

type PrePrepare struct {
	View           uint
	SequenceNumber uint
	// Block          Block
	Request   Request
	Digest    string // Block hash
	Signature []byte // Need or not ?
}

//var ErrConflictingPreprepare = errors.New("conflicting pre-prepare")

// Gửi preprepare đi
// Replica chưa có
func (r *Replica) sendPrePrepare(ctx context.Context, req Request) error {
	// Chỉ có primary node mới dc gửi
	if !r.IsPrimary() {
		return nil
	}

	r.sequence++
	sequence := r.sequence

	msg := PrePrepare{
		View:           r.view,
		SequenceNumber: sequence,
		// Block:          block,
		Request: req,
		Digest:  getDigest(req),
	}

	log := r.log.With().
		Uint("view", msg.View).
		Uint("sequence_number", msg.SequenceNumber).
		Str("digest", msg.Digest).
		Logger()

	if r.conflictingPrePrepare(msg) {
		return fmt.Errorf("dropping pre-prepare as we have a conflicting one")
	}

	// Ký thông điệp
	// err := r.sign(&msg)
	// if err != nil {
	// 	return fmt.Errorf("could not sign pre-prepare message: %w", err)
	// }

	// Phát broadcast thông điệp đến các node khác
	log.Info().Msg("broadcasting pre-prepare message")
	err := r.broadcast(ctx, &msg)
	if err != nil {
		return fmt.Errorf("could not broadcast pre-prepare message: %w", err)
	} else {
		log.Info().Msg("pre-prepare message successfully broadcast")
	}

	// Ghi nhận thông điệp này
	r.preprepares[getMessageID(msg.View, msg.SequenceNumber)] = msg

	return nil
}

// Nhận preprepare rồi check tính hợp lệ
func (r *Replica) processPrePrepare(ctx context.Context, replica peer.ID, msg PrePrepare) error {
	// Primary node k được nhận thông điệp
	if r.IsPrimary() {
		r.log.Warn().Msg("primary replica received a pre-prepare, dropping")
		return nil
	}

	log := r.log.With().
		Str("replica", replica.String()).
		Uint("view", msg.View).
		Uint("sequence_no", msg.SequenceNumber).
		Str("digest", msg.Digest).
		Logger()

	log.Info().Msg("received pre-prepare message")

	// Kiểm tra node gửi có phải là primary không
	if replica != r.primaryReplicaID() { // pbft.go
		log.Error().Str("primary", r.primaryReplicaID().String()).Msg("pre-prepare came from a replica that is not the primary, dropping")
		return nil
	}

	// Kiểm tra xem view có hợp lệ không
	if msg.View != r.view {
		return fmt.Errorf("pre-prepare for an invalid view (received: %v, current: %v)", msg.View, r.view)
	}

	// Xác minh chữ ký của thông điệp
	// err := r.verifySignature(&msg, r.primaryReplicaID())
	// if err != nil {
	// 	return fmt.Errorf("pre-prepare message signature not valid: %w", err)
	// }

	// Kiểm tra nếu đã có block đó rồi
	id := getMessageID(msg.View, msg.SequenceNumber)
	existing, ok := r.preprepares[id]
	if ok {
		log.Error().Str("existing_digest", existing.Digest).Msg("pre-prepare message already exists for this view and sequence number, dropping")
		return ErrConflictingPreprepare
	}

	// Nếu chưa có block đó -> block mới
	// Lưu block từ thông điệp PrePrepare
	r.preprepares[id] = msg

	r.requests[msg.Digest] = msg.Request
	r.pending[msg.Digest] = msg.Request

	r.startRequestTimer(false)

	if !r.prePrepared(msg.View, msg.SequenceNumber, msg.Digest) {
		log.Warn().Msg("request is not pre-prepared, stopping")
		return nil
	}
	log.Info().Msg("processed pre-prepare")

	// Phát thông điệp Prepare
	err := r.sendPrepare(ctx, msg)
	if err != nil {
		return fmt.Errorf("could not send prepare message: %w", err)
	}

	return r.maybeSendCommit(ctx, msg.View, msg.SequenceNumber, msg.Digest)
}

func (r *Replica) conflictingPrePrepare(preprepare PrePrepare) bool {

	for _, pp := range r.preprepares {
		if pp.View == preprepare.View && pp.Digest == preprepare.Digest && pp.SequenceNumber != preprepare.SequenceNumber {
			return true
		}
	}

	return false
}
