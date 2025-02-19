package db

import (
	"github.com/ethereum/go-ethereum/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var (
	blockKey    = []byte("block")
	headerKey   = []byte("header")
	receiptKey  = []byte("receipt")
	ErrEmptyKey = errors.New("key could not be empty")
)

type Level struct {
	ldb      *leveldb.DB
	quitChan chan struct{}
}

// New leveldb
func NewLevelDB(path string) (*Level, error) {
	db, err := leveldb.OpenFile(path, nil)
	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(path, nil)
	}
	if err != nil {
		log.Info("init leveldb error:", err.Error())
		return nil, err
	}
	result := &Level{
		ldb:      db,
		quitChan: make(chan struct{}),
	}
	return result, nil
}

func (l *Level) Close() {
	close(l.quitChan)
	err := l.ldb.Close()
	if err != nil {
		log.Info("Close leveldb error:", err.Error())
		return
	}
}

func (l *Level) GetString(key string) (string, error) {
	value, err := l.Get([]byte(key))
	return string(value), err
}

func (l *Level) Get(key []byte) ([]byte, error) {
	return l.ldb.Get(key, nil)
}

func (l *Level) GetBlock() ([]byte, error) {
	return l.Get(blockKey)
}

func (l *Level) GetHeader() ([]byte, error) {
	return l.Get(headerKey)
}

func (l *Level) GetReceipt() ([]byte, error) {
	return l.Get(receiptKey)
}

func (l *Level) PutBlock(value []byte) error {
	return l.ldb.Put(blockKey, value, nil)
}

func (l *Level) PutHeader(value []byte) error {
	return l.ldb.Put(headerKey, value, nil)
}

func (l *Level) PutReceipt(value []byte) error {
	return l.ldb.Put(receiptKey, value, nil)
}

func (l *Level) Put(key []byte, value []byte) error {
	if len(key) < 1 {
		return ErrEmptyKey
	}
	return l.ldb.Put(key, value, nil)
}

func (l *Level) PutString(key string, value string) error {
	return l.Put([]byte(key), []byte(value))
}

func (l *Level) Has(key []byte) (ret bool, err error) {
	return l.ldb.Has(key, nil)
}

func (l *Level) HasString(key string) (ret bool, err error) {
	return l.Has([]byte(key))
}

func (l *Level) Delete(key []byte) error {
	return l.ldb.Delete(key, nil)
}

func (l *Level) DeleteString(key string) error {
	return l.Delete([]byte(key))
}

func (l *Level) Iterator() iterator.Iterator {
	return l.ldb.NewIterator(nil, nil)
}

func (l *Level) InitLeveldb(block []byte) {
	iter := l.Iterator()
	if !iter.Next() {
		err := l.Put(blockKey, block)
		if err != nil {
			log.Info("InitLeveldb block error:", err)
		}
	}
}
