package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	StandAloneDb *badger.DB
	conf         *config.Config
	// Your Data Here (1).
}

type StandAloneStorageReader struct {
	inner     *StandAloneStorage
	iterCount int
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		StandAloneDb: nil,
		conf:         conf,
	}
	// return nil
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	opt := badger.DefaultOptions
	opt.Dir = s.conf.DBPath
	opt.ValueDir = s.conf.DBPath
	var err error
	s.StandAloneDb, err = badger.Open(opt)
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.StandAloneDb.Close()
	// return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return nil
}
