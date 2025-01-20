package model

import (
	"math/big"

	"github.com/IBM/sarama"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type AnalysisBlockInfo struct {
	BlockNumber uint64 `json:"block_number"`
	Type        int    `json:"type"`
}

type Header struct {
	ParentHash  string `json:"parentHash"`
	UncleHash   string `json:"uncleHash"`
	Coinbase    string `json:"coinBase"`
	Root        string `json:"root"`
	TxHash      string `json:"txRootHash"`
	ReceiptHash string `json:"receiptHash"`
	Bloom       string `json:"bloom"`
	Difficulty  string `json:"difficulty"`
	BlockNumber uint64 `json:"number"`
	GasLimit    uint64 `json:"gasLimit"`
	GasUsed     uint64 `json:"gasUsed"`
	Time        uint64 `json:"timestamp"`
	Extra       string `json:"extra"`
	MixDigest   string `json:"mixDigest"`
	Nonce       string `json:"nonce"`
	ReceivedAt  string `json:"receivedAt"`
	BaseFee     string `json:"baseFee"`
	Hash        string `json:"hash"`
	TxSize      int    `json:"txSize"`
	Partition   string `json:"partition"`
}

type Transaction struct {
	TransactionHash string `json:"txHash"`
	Status          string `json:"status"`
	BlockNumber     uint64 `json:"number"`
	Timestamp       uint64 `json:"timeStamp"`
	From            string `json:"from"`
	To              string `json:"to"`
	Value           string `json:"value"`
	GasFeeCap       string `json:"gasFeeCap"`
	Gas             uint64 `json:"gas"`
	GasTipCap       string `json:"gasTipCap"`
	GasPrice        string `json:"gasPrice"`
	Nonce           uint64 `json:"nonce"`
	InputData       string `json:"input"`
	Cost            uint64 `json:"cost"`
	Type            string `json:"type"`
	TxIndex         int    `json:"txIndex"`
	Partition       string `json:"partition"`
}

type TxReceipt struct {
	BlockHash           common.Hash    `json:"blockHash"`
	BlockNumber         *big.Int       `json:"blockNumber"`
	ContractAddress     common.Address `json:"contractAddress"`
	CumulativeGasUsed   uint64         `json:"cumulativeGasUsed"`
	EffectiveGasPrice   *big.Int       `json:"effectiveGasPrice"`
	From                common.Address `json:"from"`
	GasUsed             uint64         `json:"gasUsed"`
	L1BaseFeeScalar     *uint64        `json:"l1BaseFeeScalar"`
	L1BlobBaseFee       *big.Int       `json:"l1BlobBaseFee"`
	L1BlobBaseFeeScalar *uint64        `json:"l1BlobBaseFeeScalar"`
	L1Fee               *big.Int       `json:"l1Fee"`
	L1GasPrice          *big.Int       `json:"l1GasPrice"`
	L1GasUsed           *big.Int       `json:"l1GasUsed"`
	LogsBloom           types.Bloom    `json:"logsBloom"`
	Logs                []*types.Log   `json:"logs"`
	Status              uint64         `json:"status"`
	To                  common.Address `json:"to"`
	TransactionHash     common.Hash    `json:"transactionHash"`
	TransactionIndex    uint           `json:"transactionIndex"`
	Type                uint8          `json:"type"`
	Partition           string         `json:"partition"`
}

type Log struct {
	LogAddress  string   `json:"logAddress"`
	Topics      []string `json:"topics"`
	Logs        []string `json:"logs"`
	BlockNumber uint64   `json:"number"`
	TxHash      string   `json:"txHash"`
	TxIndex     int      `json:"txIndex"`
	BlockHash   string   `json:"blockHash"`
	LogIndex    int      `json:"logIndex"`
	Removed     string   `json:"removed"`
	LogId       string   `json:"logId"`
	Time        uint64   `json:"timestamp"`
	Partition   string   `json:"partition"`
}

type KafkaMessage struct {
	Messages []*sarama.ProducerMessage
}
