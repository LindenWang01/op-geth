package config

type AnalysisConfig struct {
	KafkaHosts    []string
	LevelDBPath   string
	BlockNumber   uint64 // Block begin height
	HeaderNumber  uint64 // Hearder begin height
	ReceiptNumber uint64 // Receipt begin height
}
