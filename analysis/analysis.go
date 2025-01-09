package analysis

import (
	"github.com/ethereum/go-ethereum/analysis/config"
	"github.com/ethereum/go-ethereum/analysis/process"
	"github.com/ethereum/go-ethereum/core"
)

type Analysis struct {
	blockChain    *core.BlockChain
	blockAnalysis *process.BlockDataAnalysis
	config        *config.AnalysisConfig
}

func New(config *config.AnalysisConfig, blockChain *core.BlockChain) *Analysis {
	return &Analysis{
		blockChain:    blockChain,
		blockAnalysis: process.NewBlockDataAnalysis(config, blockChain),
		config:        config,
	}
}

// Start blockchain data analysis service
func (a *Analysis) Start() {
	a.blockAnalysis.Start()
}

func (a *Analysis) Stop() {
	// Shutdown analysis service
	a.blockAnalysis.Stop()
}
