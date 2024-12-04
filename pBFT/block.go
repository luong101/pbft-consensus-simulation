package mainpBFT

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Block struct {
	PreviousBlockHash string
	BlockHeight       int
	BlockHash         string
}

func generateHash(previousBlockHash string, blockHeight int) string {
	data := fmt.Sprintf("%s%d", previousBlockHash, blockHeight)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func NewBlock(previousBlockHash string, blockHeight int) Block {
	blockHash := generateHash(previousBlockHash, blockHeight)
	return Block{
		PreviousBlockHash: previousBlockHash,
		BlockHeight:       blockHeight,
		BlockHash:         blockHash,
	}
}
