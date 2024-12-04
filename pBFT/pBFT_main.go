package mainpBFT

import "fmt"

func main() {
	genesisBlock := NewBlock("0", 0)
	fmt.Printf("Genesis Block: %+v\n", genesisBlock)

	nextBlock := NewBlock(genesisBlock.BlockHash, 1)
	fmt.Printf("Next Block: %+v\n", nextBlock)

}
