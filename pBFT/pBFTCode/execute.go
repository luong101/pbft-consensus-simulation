package pbft

import (
	"context"
	"fmt"
)

// Execute add block
func exeAddBlock(chainFile string, digest string) error {
	blocks, err := loadBlockchainFromFile(chainFile)
	if err != nil {
		return fmt.Errorf("failed to load blockchain: %w", err)
	}

	// Lấy block cuối
	lastBlock := blocks[len(blocks)-1]

	// Tạo block mới
	newBlock := Block{
		PreviousBlockHash: lastBlock.BlockHash,
		BlockHeight:       lastBlock.BlockHeight + 1,
		BlockHash:         digest,
	}

	// Ghi block mới vào file
	if err := appendBlockToBlockchain(chainFile, newBlock); err != nil {
		return fmt.Errorf("failed to append new block: %w", err)
	}

	fmt.Printf("Added new block: %+v\n", newBlock)
	return nil
}

// execute executes the request AND sends the result back to origin.
func (r *Replica) execute(ctx context.Context, view uint, sequence uint, digest string) error {

	// Đảm bảo rằng yêu cầu có tồn tại trong danh sách các yêu cầu đã nhận (r.requests)
	request, ok := r.requests[digest]
	if !ok {
		return fmt.Errorf("unknown request (digest: %s)", digest)
	}

	log := r.log.With().Uint("view", view).Uint("sequence", sequence).Str("digest", digest).Str("request", request.ID).Logger()

	// Nếu yêu cầu không có trong danh sách pending, có khả năng yêu cầu đã được thực thi trước đó và sẽ không tiếp tục xử lý
	_, havePending := r.pending[digest]
	if !havePending {
		log.Warn().Msg("no pending request with matching info - likely already executed")
		return nil
	}

	// Đảm bảo yêu cầu được thực thi theo đúng thứ tự (theo số thứ tự sequence)
	if sequence != r.lastExecuted+1 {
		log.Error().Msg("requests with lower sequence number have not been executed")
		return nil
	}

	// Sanity check - should never happen.
	if sequence < r.lastExecuted {
		log.Error().Uint("last_executed", r.lastExecuted).Msg("requests executed out of order!")
	}

	// Xóa request khỏi danh sách pending
	delete(r.pending, digest)

	log.Info().Msg("executing request")

	// Thực hiện lưu Block
	err := exeAddBlock(r.chainFile, digest)
	if err != nil {
		log.Error().Err(err).Msg("execution add block failed")
	}

	// Stop the timer since we completed an execution.
	r.stopRequestTimer()

	// If we have more pending requests, start a new timer.
	if len(r.pending) > 0 {
		r.startRequestTimer(true)
	}

	log.Info().Msg("executed request")

	fmt.Println(ctx)

	return nil
}
