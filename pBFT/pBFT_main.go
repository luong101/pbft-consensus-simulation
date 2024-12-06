package mainpBFT

import "fmt"

func tmp() {
	file := "blockchain.txt"

	// Khởi tạo blockchain, nếu file đã tồn tại thì không làm gì
	err := newBlockchain(file)
	if err != nil {
		fmt.Println("Error initializing blockchain:", err)
		return
	}

	// Đọc chuỗi block từ file
	blocks, err := loadBlockchainFromFile(file)
	if err != nil {
		fmt.Println("Error loading blockchain:", err)
		return
	}

	// Thêm block mới dô
	newBlock := NewBlock(blocks[0].BlockHash, 1)
	err = appendBlockToBlockchain(file, newBlock)
	if err != nil {
		fmt.Println("Error append block to file:", err)
		return
	}

	// Đọc chuỗi block từ file
	blocks, err = loadBlockchainFromFile(file)
	if err != nil {
		fmt.Println("Error loading blockchain:", err)
		return
	}

	// In thông tin về các block
	for _, block := range blocks {
		fmt.Printf("BlockHeight: %d, Hash: %s, PrevHash: %s\n",
			block.BlockHeight, block.BlockHash, block.PreviousBlockHash)
	}
}

func MainpBFT() {
	
}
