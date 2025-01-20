package config

type AnalysisConfig struct {
	KafkaHosts    []string
	LevelDBPath   string
	HeaderNumber  uint64 // Analysis begin height
	ReceiptNumber uint64 // Receipt begin height
}
