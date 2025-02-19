package process

import (
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/ethereum/go-ethereum/analysis/kafka"
	"github.com/ethereum/go-ethereum/analysis/model"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	topicHeader        = "header"
	topicTx            = "transaction"
	topicReceipt       = "receipt"
	topicLog           = "log"
	txNumber           = 10
	logNumber          = 40
	timeLayoutYYYYMMDD = "20060102"
)

var (
	systemAddress = common.HexToAddress("0xDeaDDEaDDeAdDeAdDEAdDEaddeAddEAdDEAd0001")
)

type Process struct {
	kafkaScheduler  *kafka.KafkaScheduler
	headerLastTime  int64
	headerDate      string
	receiptLastTime int64
	receiptData     string
}

// analysisHeader
func (p *Process) analysisHeader(block *types.Block) *model.Header {
	if block.Header() != nil {
		var headerMsg = model.Header{
			ParentHash:  block.Header().ParentHash.String(),
			UncleHash:   block.Header().UncleHash.String(),
			Coinbase:    block.Header().Coinbase.String(),
			Root:        block.Header().Root.String(),
			TxHash:      block.Header().TxHash.String(),
			ReceiptHash: block.Header().ReceiptHash.String(),
			Bloom:       hexutil.Encode(block.Header().Bloom.Bytes()),
			Difficulty:  block.Header().Difficulty.String(),
			BlockNumber: block.Header().Number.Uint64(),
			GasLimit:    block.Header().GasLimit,
			GasUsed:     block.Header().GasUsed,
			Time:        block.Header().Time,
			Extra:       hexutil.Encode(block.Header().Extra),
			MixDigest:   block.Header().MixDigest.String(),
			Nonce:       strconv.FormatUint(block.Header().Nonce.Uint64(), 10),
			ReceivedAt:  block.ReceivedAt.String(),
			Hash:        block.Header().Hash().String(),
			TxSize:      block.Transactions().Len(),
		}
		if block.Header().BaseFee != nil {
			headerMsg.BaseFee = block.Header().BaseFee.String()
		}
		return &headerMsg
	}
	return nil
}

// analysisTransaction
func (p *Process) analysisTransaction(transactions types.Transactions, time, blockNumber uint64) []*model.Transaction {
	var parseTransactions = make([]*model.Transaction, 0)
	for i, tx := range transactions {
		singer := types.LatestSignerForChainID(tx.ChainId())
		from, err := singer.Sender(tx)
		var txFromStr string
		if err == nil {
			txFromStr = from.String()
		}
		var to, value, gasFeeCap, gasTipCap, gasPrice string
		var cost uint64
		if tx.To() != nil {
			to = tx.To().String()
		}

		// Ignore system address to L1BlockAddr(Set L1Block Values Ecotone)
		if from.Cmp(systemAddress) == 0 && to == types.L1BlockAddr.String() {
			continue
		}

		if tx.Value() != nil {
			value = tx.Value().String()
		}
		if tx.GasFeeCap() != nil {
			gasFeeCap = tx.GasFeeCap().String()
		}
		if tx.GasTipCap() != nil {
			gasTipCap = tx.GasTipCap().String()
		}
		if tx.GasPrice() != nil {
			gasPrice = tx.GasPrice().String()
		}
		if tx.Cost() != nil {
			cost = tx.Cost().Uint64()
		}
		var transactionMsg = model.Transaction{
			TransactionHash: tx.Hash().String(),
			BlockNumber:     blockNumber,
			From:            txFromStr,
			To:              to,
			Value:           value,
			GasFeeCap:       gasFeeCap,
			Gas:             tx.Gas(),
			GasTipCap:       gasTipCap,
			GasPrice:        gasPrice,
			Timestamp:       time,
			Nonce:           tx.Nonce(),
			InputData:       hexutil.Encode(tx.Data()),
			Cost:            cost,
			Type:            strconv.Itoa(int(tx.Type())),
			TxIndex:         i,
		}
		parseTransactions = append(parseTransactions, &transactionMsg)
	}
	return parseTransactions
}

// analysisTxReceipt
func (p *Process) analysisTxReceipt(receipts types.Receipts, block *types.Block) []*model.TxReceipt {
	var parseReceipts = make([]*model.TxReceipt, 0)

	txMap := make(map[common.Hash]*types.Transaction)
	for _, tx := range block.Transactions() {
		txMap[tx.Hash()] = tx
	}

	for _, receipt := range receipts {
		var from, to common.Address
		// Find the corresponding transaction using receipt's TxHash
		tx := txMap[receipt.TxHash]
		if tx != nil {
			signer := types.LatestSignerForChainID(tx.ChainId())
			from, _ = signer.Sender(tx)

			if tx.To() != nil {
				to = *tx.To()
			}

			// Ignore system address to L1BlockAddr(Set L1Block Values Ecotone)
			if from.Cmp(systemAddress) == 0 && to.Cmp(types.L1BlockAddr) == 0 {
				continue
			}
		}
		// logs := p.analysisLog(receipt.Logs)
		var postState = ""
		if len(receipt.PostState) > 0 {
			postState = string(receipt.PostState)
		}

		receiptMsg := model.TxReceipt{
			BlockHash:           receipt.BlockHash,
			BlockNumber:         receipt.BlockNumber,
			ContractAddress:     receipt.ContractAddress,
			CumulativeGasUsed:   receipt.CumulativeGasUsed,
			EffectiveGasPrice:   receipt.EffectiveGasPrice,
			From:                from,
			GasUsed:             receipt.GasUsed,
			Logs:                receipt.Logs,
			L1BaseFeeScalar:     receipt.L1BaseFeeScalar,
			L1BlobBaseFee:       receipt.L1BlobBaseFee,
			L1BlobBaseFeeScalar: receipt.L1BlobBaseFeeScalar,
			L1Fee:               receipt.L1Fee,
			L1GasPrice:          receipt.L1GasPrice,
			L1GasUsed:           receipt.L1GasUsed,
			LogsBloom:           receipt.Bloom,
			Status:              receipt.Status,
			To:                  to,
			TransactionHash:     receipt.TxHash,
			TransactionIndex:    receipt.TransactionIndex,
			Type:                receipt.Type,
			PostState:           postState,
		}

		parseReceipts = append(parseReceipts, &receiptMsg)
	}
	return parseReceipts
}

// analysisLog
func (p *Process) analysisLog(logs []*types.Log) []*model.Log {
	var parseLogs = make([]*model.Log, 0)
	for _, log := range logs {
		var logTopics = make([]string, 0)
		for _, pic := range log.Topics {
			logTopics = append(logTopics, pic.String())
		}
		remover := "0"
		if log.Removed {
			remover = "1"
		}
		lodData := hexutil.Encode(log.Data)
		lodData = strings.ReplaceAll(lodData, "0x", "")
		var le = len(lodData) / 64
		var logDatas = make([]string, 0)
		for i := 0; i < le; i++ {
			var start = i * 64
			var end = (i + 1) * 64
			logDatas = append(logDatas, lodData[start:end])
		}
		logData := model.Log{
			LogAddress: log.Address.String(),
			Topics:     logTopics,
			Logs:       logDatas,
			LogIndex:   int(log.Index),
			Removed:    remover,
		}
		parseLogs = append(parseLogs, &logData)
	}
	return parseLogs
}

// sendMsgChain
func (p *Process) sendMsgChain(m *sarama.ProducerMessage) {
	p.kafkaScheduler.KafkaMessage <- &model.Message{
		Message: m,
	}
}

// GetLastTime  get the date last time
func (p *Process) GetLastTime(blockTime int64) (int64, string) {
	t2 := time.Unix(blockTime, 0).Format(timeLayoutYYYYMMDD)
	loc, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation(timeLayoutYYYYMMDD, t2, loc)
	// Last second of a day
	endTime := time.Date(theTime.Year(), theTime.Month(), theTime.Day(), 23, 59, 59, 0, theTime.Location()).Unix()
	return endTime, t2
}
