package mainpBFT

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Commit struct {
	View           uint
	SequenceNumber uint
	Digest         string
	Signature      string
}

// Xem xét xem node có cần gửi Commit message không
func (r *Replica) maybeSendCommit(ctx context.Context, view uint, sequenceNo uint, digest string) error {
	log := r.log.With().Uint("view", view).Uint("sequence_number", sequenceNo).Str("digest", digest).Logger()

	if !r.shouldSendCommit(view, sequenceNo, digest) {
		log.Info().Msg("not sending commit")
		return nil
	}

	// Broadcast Commit message
	log.Info().Msg("request prepared, broadcasting commit")
	err := r.sendCommit(ctx, view, sequenceNo, digest)
	if err != nil {
		return fmt.Errorf("could not send commit message: %w", err)
	}

	// Kiểm tra xem đã đạt quorum Commit chưa
	if !r.committed(view, sequenceNo, digest) {
		log.Info().Msg("request is not yet committed")
		return nil
	}
	log.Info().Msg("request committed, executing")

	return r.execute(ctx, view, sequenceNo, digest)
}

// Quyết định xem có nên gửi Commit không
func (r *Replica) shouldSendCommit(view uint, sequenceNo uint, digest string) bool {
	log := r.log.With().Uint("view", view).Uint("sequence_number", sequenceNo).Str("digest", digest).Logger()

	// Check xem đã đặt quorum Prepare chưa
	if !r.prepared(view, sequenceNo, digest) {
		log.Info().Msg("request not yet prepared, commit not due yet")
		return false
	}

	// Kiểm tra xem đã gửi commit chưa
	msgID := getMessageID(view, sequenceNo)
	commits, ok := r.commits[msgID]
	if ok {
		_, sent := commits.m[r.id]
		if sent {
			log.Info().Msg("commit for this request already broadcast")
			return false
		}
	}

	return true
}

// Broadcast Commit message cho các node
func (r *Replica) sendCommit(ctx context.Context, view uint, sequenceNo uint, digest string) error {
	log := r.log.With().Uint("view", view).Uint("sequence_number", sequenceNo).Str("digest", digest).Logger()

	log.Info().Msg("broadcasting commit message")

	commit := Commit{
		View:           view,
		SequenceNumber: sequenceNo,
		Digest:         digest,
	}

	// Bỏ qua sign

	// Broadcast Commit message
	err := r.broadcast(ctx, &commit)
	if err != nil {
		return fmt.Errorf("could not broadcast commit message: %w", err)
	}
	log.Info().Msg("commit message successfully broadcast")

	// Record Commit message
	r.recordCommitReceipt(r.id, commit)

	return nil
}

// Xử lý Commit message nhận được
func (r *Replica) processCommit(ctx context.Context, replica peer.ID, commit Commit) error {
	log := r.log.With().Str("replica", replica.String()).Uint("view", commit.View).Uint("sequence_no", commit.SequenceNumber).Str("digest", commit.Digest).Logger()

	log.Info().Msg("received commit message")

	// Xác minh View
	if commit.View != r.view {
		return fmt.Errorf("commit has an invalid view value (received: %v, current: %v)", commit.View, r.view)
	}

	// Bỏ qua xác minh sign

	// Ghi nhận Commit message
	r.recordCommitReceipt(replica, commit)

	// Kiểm tra xem đã được Commit đủ chưa
	if !r.committed(commit.View, commit.SequenceNumber, commit.Digest) {
		log.Info().Msg("request is not yet committed")
		return nil
	}

	// Nếu Committed thì thực thi yêu cầu
	err := r.execute(ctx, commit.View, commit.SequenceNumber, commit.Digest)
	if err != nil {
		return fmt.Errorf("request execution failed: %w", err)

	}

	return nil
}

// Record Commit message từ 1 Replica
func (r *Replica) recordCommitReceipt(replica peer.ID, commit Commit) {
	// Ghi nhận msgID
	msgID := getMessageID(commit.View, commit.SequenceNumber)

	// Nếu chưa có message nào có msgID thì lưu vào
	commits, ok := r.commits[msgID]
	if !ok {
		r.commits[msgID] = newCommitReceipts()
		commits = r.commits[msgID]
	}

	// Dùng lock để tránh xung đột khi ghi
	commits.Lock()
	defer commits.Unlock()

	// Bỏ qua nếu đã có message dc gửi trước đó
	_, exists := commits.m[replica]
	if exists {
		r.log.Warn().Uint("view", commit.View).Uint("sequence", commit.SequenceNumber).Str("digest", commit.Digest).Msg("ignoring duplicate commit")
		return
	}

	commits.m[replica] = commit
}
