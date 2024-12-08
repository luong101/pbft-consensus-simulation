// package pbft
package pbft

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Block struct {
	PreviousBlockHash string
	BlockHeight       int
	BlockHash         string
}

func NewBlockWithPrevBlock(previousBlockHash string, blockHeight int, blockHash string) Block {
	// blockHash := generateHash(previousBlockHash, blockHeight, data)
	return Block{
		PreviousBlockHash: previousBlockHash,
		BlockHeight:       blockHeight,
		BlockHash:         blockHash,
	}
}

type Blockchain struct {
	Chain []Block
}

func newBlockWithPrevBlockchain(chainFile string) error {
	// Check coi tập tin tồn tại chưa
	_, err := os.Stat(chainFile)
	if err == nil {
		// Nếu file đã tồn tại, không làm gì
		return nil
	}

	// Nếu chưa có file thì tạo file
	f, err := os.OpenFile(chainFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}
	defer f.Close()

	// Add block genesis
	genesisBlock := NewBlockWithPrevBlock("", 0, "")
	if err := appendBlockToBlockchain(chainFile, genesisBlock); err != nil {
		return fmt.Errorf("could not write genesis block: %w", err)
	}
	return nil
}

func appendBlockToBlockchain(chainFile string, block Block) error {
	f, err := os.OpenFile(chainFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(fmt.Sprintf("%s,%d,%s\n", block.PreviousBlockHash, block.BlockHeight, block.BlockHash))
	return err
}

func loadBlockchainFromFile(chainFile string) ([]Block, error) {
	// Mở file ở chế độ chỉ đọc
	f, err := os.Open(chainFile)
	if err != nil {
		return nil, fmt.Errorf("could not open blockchain file: %w", err)
	}
	defer f.Close()

	var blocks []Block

	// Đọc từng dòng trong file và parse thành block
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		block, err := parseBlock(line)
		if err != nil {
			return nil, fmt.Errorf("could not parse block: %w", err)
		}
		blocks = append(blocks, block)
	}

	// Kiểm tra lỗi quét file
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("could not read blockchain file: %w", err)
	}

	return blocks, nil
}

func parseBlock(line string) (Block, error) {
	parts := strings.Split(line, ",")
	if len(parts) != 3 {
		return Block{}, fmt.Errorf("invalid block format")
	}

	blockHeight, err := strconv.Atoi(parts[1])
	if err != nil {
		return Block{}, fmt.Errorf("invalid block height: %w", err)
	}

	return Block{
		PreviousBlockHash: parts[0],
		BlockHeight:       blockHeight,
		BlockHash:         parts[2],
	}, nil
}
