package config

type AnalysisConfig struct {
	KafkaHost     string
	LevelDBPath   string
	HeaderNumber  uint64 // Analysis begin height
	ReceiptNumber uint64 // Receipt begin height
}
