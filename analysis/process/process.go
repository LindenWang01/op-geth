package process

import (
	"encoding/json"
	"fmt"
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
	topicHeader  = "header"
	topicTx      = "transaction"
	topicReceipt = "receipt"
	topicLog     = "log"
	txNumber     = 10
	logNumber    = 40
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
func (p *Process) analysisHeader(block *types.Block, typeName string) {
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
	topic := "base_" + typeName
	topicp := p.getTopicName(int64(block.Header().Time), typeName)
	headerMsg.Partition = p.partition(topicp)
	msg, _ := json.Marshal(headerMsg)
	var messages = make([]*sarama.ProducerMessage, 0)
	m := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	messages = append(messages, &m)
	p.sendKafkaMsgChain(messages)
}

// analysisTransaction
func (p *Process) analysisTransaction(transactions types.Transactions, typeName string, time, blockNumber uint64) {
	var txIndex = 0
	topicp := p.getTopicName(int64(time), typeName)
	topic := "base_" + typeName
	var messages = make([]*sarama.ProducerMessage, 0)
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
			TxIndex:         txIndex,
		}
		transactionMsg.Partition = p.partition(topicp)
		msg, _ := json.Marshal(transactionMsg)
		m := sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		}
		messages = append(messages, &m)
		if i+1%txNumber == 0 {
			p.sendKafkaMsgChain(messages)
			messages = make([]*sarama.ProducerMessage, 0)
		}
		txIndex++
	}
	if len(messages) > 0 {
		p.sendKafkaMsgChain(messages)
	}
}

// analysisTxReceipt
func (p *Process) analysisTxReceipt(receipts types.Receipts, block *types.Block, typeName string) {
	topicp := p.getTopicName(int64(block.Time()), typeName)
	topic := "base_" + typeName
	var messages = make([]*sarama.ProducerMessage, 0)

	// Create a mapping from transaction hash to transaction for quick lookup
	txMap := make(map[common.Hash]*types.Transaction)
	for _, tx := range block.Transactions() {
		txMap[tx.Hash()] = tx
	}

	for i, receipt := range receipts {
		// p.analysisLog(receipt.Logs, topicLog, time)

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
		}

		receiptMsg.Partition = p.partition(topicp)
		msg, _ := json.Marshal(receiptMsg)
		m := sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		}
		messages = append(messages, &m)
		if i+1%txNumber == 0 {
			p.sendKafkaMsgChain(messages)
			messages = make([]*sarama.ProducerMessage, 0)
		}
	}
	if len(messages) > 0 {
		p.sendKafkaMsgChain(messages)
	}
}

// analysisLog
func (p *Process) analysisLog(logs []*types.Log, typeName string, time uint64) {
	topicp := p.getTopicName(int64(time), typeName)
	topic := "base_" + typeName
	partition := p.partition(topicp)
	var messages = make([]*sarama.ProducerMessage, 0)
	for i, log := range logs {
		var logTopics = make([]string, 0)
		for _, pic := range log.Topics {
			logTopics = append(logTopics, pic.String())
		}
		remover := "0"
		if log.Removed {
			remover = "1"
		}
		number := strconv.FormatUint(log.BlockNumber, 10)
		txIndex := strconv.FormatInt(int64(log.TxIndex), 10)
		logIndex := strconv.FormatInt(int64(log.Index), 10)
		LogId := number + "/" + txIndex + "/" + logIndex
		lodData := hexutil.Encode(log.Data)
		lodData = strings.ReplaceAll(lodData, "0x", "")
		var le = len(lodData) / 64
		var logDatas = make([]string, 0)
		for i := 0; i < le; i++ {
			var start = i * 64
			var end = (i + 1) * 64
			logDatas = append(logDatas, lodData[start:end])
		}
		bscLogData := model.Log{
			LogAddress:  log.Address.String(),
			Topics:      logTopics,
			TxHash:      log.TxHash.String(),
			Logs:        logDatas,
			BlockNumber: log.BlockNumber,
			TxIndex:     int(log.TxIndex),
			BlockHash:   log.BlockHash.String(),
			LogIndex:    int(log.Index),
			Time:        time,
			Removed:     remover,
			LogId:       LogId,
			Partition:   partition,
		}
		msg, _ := json.Marshal(bscLogData)
		m := sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		}
		messages = append(messages, &m)
		if i+1%logNumber == 0 {
			p.sendKafkaMsgChain(messages)
			messages = make([]*sarama.ProducerMessage, 0)
		}
	}
	if len(messages) > 0 {
		p.sendKafkaMsgChain(messages)
	}
}

// getTopicName
func (p *Process) getTopicName(blockTime int64, typeName string) string {
	topic := "base_" + typeName + "-"
	if typeName == topicHeader || typeName == topicTx {
		if blockTime > p.headerLastTime {
			endTime, today := p.GetLastTime(blockTime)
			p.headerLastTime = endTime
			p.headerDate = today
		}
		topic += p.headerDate
	} else {
		if blockTime > p.receiptLastTime {
			endTime, today := p.GetLastTime(blockTime)
			p.receiptLastTime = endTime
			p.receiptData = today
		}
		topic += p.receiptData
	}
	return topic
}

// sendKafkaMsgChain
func (p *Process) sendKafkaMsgChain(m []*sarama.ProducerMessage) {
	p.kafkaScheduler.KafkaMessage <- &model.KafkaMessage{
		Messages: m,
	}
}

// partition
func (p *Process) partition(topic string) string {
	var part = ""
	if len(topic) > 0 && strings.Contains(topic, "-") {
		topics := strings.Split(topic, "-")[1]
		part = fmt.Sprintf("%s/%s/%s", topics[0:4], topics[4:6], topics[6:8])
	}
	return part
}

// GetLastTime  get the date last time
func (p *Process) GetLastTime(blockTime int64) (int64, string) {
	timeLayout := "20060102"
	t2 := time.Unix(blockTime, 0).Format(timeLayout)
	loc, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation(timeLayout, t2, loc)
	// Last second of a day
	endTime := time.Date(theTime.Year(), theTime.Month(), theTime.Day(), 23, 59, 59, 0, theTime.Location()).Unix()
	return endTime, t2
}
