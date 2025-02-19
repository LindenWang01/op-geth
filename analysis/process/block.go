package process

import (
	"context"
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/ethereum/go-ethereum/analysis/config"
	"github.com/ethereum/go-ethereum/analysis/db"
	"github.com/ethereum/go-ethereum/analysis/kafka"
	"github.com/ethereum/go-ethereum/analysis/model"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/log"
	"github.com/klauspost/compress/zstd"
)

const (
	refreshTime                = 2 * time.Second
	blockWaitTime              = 2 * time.Second
	headerWaitTime             = 2 * time.Second
	receiptWaitTime            = 2 * time.Second
	updateTime                 = 10 * time.Second
	blockChainSafeConfirmCount = 20
	topicBlock                 = "base_block"
)

type BlockDataAnalysis struct {
	ctx            context.Context
	cancel         context.CancelFunc
	config         *config.AnalysisConfig
	blockInfo      *model.AnalysisRecord
	process        Process
	kafkaScheduler *kafka.KafkaScheduler
	levelDb        *db.Level
	blockChain     *core.BlockChain
	currentNumber  uint64
	processNumber  uint64
}

func NewBlockDataAnalysis(config *config.AnalysisConfig, blockChain *core.BlockChain) *BlockDataAnalysis {
	client, err := kafka.NewKafkaScheduler(config.KafkaHosts)
	if err != nil {
		log.Crit("NewKafkaScheduler", "error", err)
	}

	client.KafkaHosts = config.KafkaHosts
	ctx, cancel := context.WithCancel(context.Background())
	ld, block, err := initLevelDb(config.LevelDBPath, config.BlockNumber)
	if err != nil {
		log.Crit("initLevelDb", "error", err)
	}

	b := &BlockDataAnalysis{
		ctx:       ctx,
		cancel:    cancel,
		config:    config,
		blockInfo: block,
		process: Process{
			kafkaScheduler: client,
		},
		kafkaScheduler: client,
		levelDb:        ld,
		blockChain:     blockChain,
		currentNumber:  blockChain.CurrentHeader().Number.Uint64(),
		processNumber:  block.BlockNumber,
	}

	// Get analysis block numbers from level db
	// err = b.getBlockInfo()
	// if err != nil {
	// 	log.Crit("getBlockInfo", "error", err)
	// }

	return b
}

func NewApiBlockDataAnalysis(blockChain *core.BlockChain) *BlockDataAnalysis {
	return &BlockDataAnalysis{
		blockChain: blockChain,
	}
}

// func (b *BlockDataAnalysis) getBlockInfo() error {
// 	header, err := b.levelDb.GetHeader()
// 	if err != nil {
// 		return err
// 	}
// 	err = json.Unmarshal(header, b.headerBlockInfo)
// 	if err != nil {
// 		return err
// 	}
//
// 	receipt, err := b.levelDb.GetReceipt()
// 	if err != nil {
// 		return err
// 	}
// 	err = json.Unmarshal(receipt, b.receiptBlockInfo)
// 	if err != nil {
// 		return err
// 	}
//
// 	return nil
// }

func (b *BlockDataAnalysis) Start() {
	go b.refresh(b.ctx)
	log.Info("Analysis service refresh handler started.")
	go b.kafkaScheduler.Start(b.ctx)
	log.Info("Analysis service kafka scheduler started.")
	go b.start(b.ctx)
	log.Info("Analysis service start handler started.")
}

func (b *BlockDataAnalysis) Stop() {
	b.cancel()
	b.doClose()
	log.Info("Analysis service stoped")
}

func (b *BlockDataAnalysis) doClose() {
	b.levelDb.Close()
}

func (b *BlockDataAnalysis) start(ctx context.Context) {
	number := b.processNumber
	timer := time.NewTimer(blockWaitTime)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			for number <= b.currentNumber {
				block := b.AnalysisBlock(number)
				if block != nil {
					// Send data to kafka
					b.sendMsgChain(block)
					// Save analysis progress
					b.saveNumber(number)
				}
				number++
			}
			timer.Reset(blockWaitTime)
		}
	}
}

// AnalysisBlock analysis block
func (b *BlockDataAnalysis) AnalysisBlock(number uint64) *model.Block {
	block := b.blockChain.GetBlockByNumber(number)
	if block != nil {
		var parseBlock = model.Block{}
		parseBlock.Header = b.process.analysisHeader(block)
		parseBlock.Transactions = b.process.analysisTransaction(block.Transactions(), block.Time(), block.NumberU64())
		receipts := b.blockChain.GetReceiptsByHash(block.Hash())
		parseBlock.Receipts = b.process.analysisTxReceipt(receipts, block)
		return &parseBlock
	}
	return nil
}

// initLevelDb initialize leveldb
func initLevelDb(path string, blockNumber uint64) (*db.Level, *model.AnalysisRecord, error) {
	// Offset data
	ld, err := db.NewLevelDB(path)
	if err != nil {
		log.Crit("InitLevelDb", "error", err)
	}
	// initialize
	block := model.AnalysisRecord{BlockNumber: blockNumber, Type: model.BlockRecordType}
	b, _ := json.Marshal(block)
	ld.InitLeveldb(b)
	ld.Close()

	// initialize
	levelDb, err := db.NewLevelDB(path)
	if err != nil {
		log.Crit("NewLevelDB", "error", err)
	}

	blockNum, _ := levelDb.GetBlock()
	var record model.AnalysisRecord
	err = json.Unmarshal(blockNum, &record)
	return levelDb, &record, err
}

// saveNumber saves the progress of block analysis
func (b *BlockDataAnalysis) saveNumber(number uint64) {
	b.blockInfo.BlockNumber = number
	blockByte, _ := json.Marshal(b.blockInfo)
	err := b.levelDb.PutBlock(blockByte)
	if err != nil {
		log.Error("PutBlock", "error", err)
	}
}

// refresh block number
func (b *BlockDataAnalysis) refresh(ctx context.Context) {
	timer := time.NewTimer(refreshTime)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			b.currentNumber = b.blockChain.CurrentBlock().Number.Uint64()
			timer.Reset(refreshTime)
		}
	}
}

// sendMsgChain send msg
func (b *BlockDataAnalysis) sendMsgChain(block *model.Block) {
	msg, _ := json.Marshal(block)

	// Compress data
	encode, err := zstd.NewWriter(nil)
	if err != nil {
		log.Info("zstd.NewWriter:", err.Error())
	}
	dist := encode.EncodeAll(msg, nil)
	m := sarama.ProducerMessage{
		Topic: topicBlock,
		Value: sarama.StringEncoder(dist),
	}
	b.process.sendMsgChain(&m)
	encode.Close()

	// Decompress data on the consumer side
	//decoder, _ := zstd.NewReader(nil)
	//res1, err := decoder.DecodeAll(m.Value, nil)
}
