package kafka

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/ethereum/go-ethereum/analysis/model"
	"github.com/ethereum/go-ethereum/log"
)

type KafkaScheduler struct {
	KafkaHost    string
	syncProducer sarama.SyncProducer
	msgProducer  sarama.ProducerMessage
	KafkaMessage chan *model.KafkaMessage
}

// NewKafkaScheduler init kafka scheduler
func NewKafkaScheduler(addr []string) (*KafkaScheduler, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Version = sarama.V3_0_0_0
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 3
	config.Producer.Timeout = time.Minute * 3
	config.Producer.Retry.Backoff = time.Second * 2

	// create sync producer
	syncProducer, err := sarama.NewSyncProducer(addr, config)
	if err != nil {
		log.Error("Kafka syncProducer closed, error:", err)
		return nil, err
	}
	return &KafkaScheduler{
		syncProducer: syncProducer,
		KafkaMessage: make(chan *model.KafkaMessage, 10000),
	}, nil
}

// Start run kafka
func (k *KafkaScheduler) Start(ctx context.Context) {
	k.readMsg4Kafka(ctx)
}

func (k *KafkaScheduler) Close() {
	err := k.syncProducer.Close()
	if err != nil {
		return
	}
}

// readMsg run readMsg
func (k *KafkaScheduler) readMsg4Kafka(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-k.KafkaMessage:
			k.SyncSendMsg2Kafka(m)
		}
	}
}

// SyncSendToKafka sync send message
func (k *KafkaScheduler) SyncSendMsg2Kafka(m *model.KafkaMessage) {
	for _, msg := range m.Messages {
		_, _, err := k.syncProducer.SendMessage(msg)
		if err != nil {
			log.Error("Kafka syncProducer send message error:", err)
		}
	}
}
