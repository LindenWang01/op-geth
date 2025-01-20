package process

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ethereum/go-ethereum/analysis/config"
	"github.com/ethereum/go-ethereum/analysis/db"
	"github.com/ethereum/go-ethereum/analysis/kafka"
	"github.com/ethereum/go-ethereum/analysis/model"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/log"
)

const (
	refreshTime                = 2 * time.Second
	headerWaitTime             = 2 * time.Second
	receiptWaitTime            = 2 * time.Second
	updateTime                 = 10 * time.Second
	blockChainSafeConfirmCount = 20
)

type BlockDataAnalysis struct {
	ctx              context.Context
	cancel           context.CancelFunc
	config           *config.AnalysisConfig
	headerBlockInfo  *model.AnalysisBlockInfo
	receiptBlockInfo *model.AnalysisBlockInfo
	process          Process
	kafkaScheduler   *kafka.KafkaScheduler
	levelDb          *db.Level
	blockChain       *core.BlockChain
	currentNumber    uint64
}

func NewBlockDataAnalysis(config *config.AnalysisConfig, blockChain *core.BlockChain) *BlockDataAnalysis {
	client, err := kafka.NewKafkaScheduler(config.KafkaHosts)
	if err != nil {
		log.Crit("NewKafkaScheduler", "error", err)
	}

	client.KafkaHosts = config.KafkaHosts
	ctx, cancel := context.WithCancel(context.Background())
	ld, _ := initLevelDb(config.LevelDBPath, config.HeaderNumber, config.ReceiptNumber)

	b := &BlockDataAnalysis{
		ctx:              ctx,
		cancel:           cancel,
		config:           config,
		headerBlockInfo:  new(model.AnalysisBlockInfo),
		receiptBlockInfo: new(model.AnalysisBlockInfo),
		process: Process{
			kafkaScheduler: client,
		},
		kafkaScheduler: client,
		levelDb:        ld,
		blockChain:     blockChain,
		currentNumber:  blockChain.CurrentHeader().Number.Uint64(),
	}

	// Get analysis block numbers from level db
	err = b.getBlockInfo()
	if err != nil {
		log.Crit("getBlockInfo", "error", err)
	}

	return b
}

func (b *BlockDataAnalysis) getBlockInfo() error {
	header, err := b.levelDb.GetHeader()
	if err != nil {
		return err
	}
	err = json.Unmarshal(header, b.headerBlockInfo)
	if err != nil {
		return err
	}

	receipt, err := b.levelDb.GetReceipt()
	if err != nil {
		return err
	}
	err = json.Unmarshal(receipt, b.receiptBlockInfo)
	if err != nil {
		return err
	}

	return nil
}

func (b *BlockDataAnalysis) Start() {
	go b.refresh(b.ctx)
	log.Info("Analysis service refresh handler started.")
	go b.kafkaScheduler.Start(b.ctx)
	log.Info("Analysis service kafka scheduler started.")
	// go b.AnalysisHeaderAndTx(b.ctx)
	// log.Info("Analysis service header and tx started.")
	go b.AnalysisReceiptAndLog(b.ctx)
	log.Info("Analysis service receipt and log started.")
}

func (b *BlockDataAnalysis) Stop() {
	b.cancel()
	b.doClose()
	log.Info("Analysis service stoped")
}

func (b *BlockDataAnalysis) doClose() {
	b.levelDb.Close()
}

// AnalysisHeaderAndTx analysis header and transaction
func (b *BlockDataAnalysis) AnalysisHeaderAndTx(ctx context.Context) {
	number := b.headerBlockInfo.BlockNumber
	timer := time.NewTimer(headerWaitTime)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			for number <= b.currentNumber-blockChainSafeConfirmCount {
				block := b.blockChain.GetBlockByNumber(number)
				if block != nil {
					b.process.analysisHeader(block, topicHeader)
					b.process.analysisTransaction(block.Transactions(), topicTx, block.Time(), block.NumberU64())
					b.headerBlockInfo.BlockNumber = number
					h, _ := json.Marshal(b.headerBlockInfo)
					b.levelDb.PutHeader(h)
					number++
				}
			}
		}
		timer.Reset(headerWaitTime)
	}
}

// AnalysisReceiptAndLog  analysis receipt and log
func (b *BlockDataAnalysis) AnalysisReceiptAndLog(ctx context.Context) {
	number := b.receiptBlockInfo.BlockNumber
	timer := time.NewTimer(receiptWaitTime)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			for number <= b.currentNumber-blockChainSafeConfirmCount {
				block := b.blockChain.GetBlockByNumber(number)
				if block != nil {
					receipts := b.blockChain.GetReceiptsByHash(block.Hash())
					if receipts.Len() > 0 {
						b.process.analysisTxReceipt(receipts, block, topicReceipt)
					}
					b.receiptBlockInfo.BlockNumber = number
					r, _ := json.Marshal(b.receiptBlockInfo)
					b.levelDb.PutReceipt(r)
					number++
				}
			}
			timer.Reset(receiptWaitTime)
		}
	}
}

// initLevelDb initialize leveldb
func initLevelDb(path string, headerNumber, receiptNumber uint64) (*db.Level, error) {
	// Offset data
	ld, err := db.NewLevelDB(path)
	if err != nil {
		log.Crit("InitLevelDb", "error", err)
	}
	// initialize
	var (
		header  = model.AnalysisBlockInfo{BlockNumber: headerNumber, Type: model.HeaderRecordType}
		receipt = model.AnalysisBlockInfo{BlockNumber: receiptNumber, Type: model.ReceiptRecordType}
	)
	h, _ := json.Marshal(header)
	r, _ := json.Marshal(receipt)
	ld.InitLeveldb(h, r)
	ld.Close()

	// initialize
	levelDb, err := db.NewLevelDB(path)
	if err != nil {
		log.Crit("NewLevelDB", "error", err)
	}
	return levelDb, err
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
