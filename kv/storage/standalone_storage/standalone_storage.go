package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
	// Your Data Here (1).
}

type StandAloneStorageReader struct {
	inner *StandAloneStorage
	txn   *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(sr.txn, cf, key)
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Commit()
	sr.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{
		engine: engine_util.NewEngines(db, nil, conf.DBPath, ""),
	}
}

func NewStandAloneStorageReader(s *StandAloneStorage, t *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		inner: s,
		txn:   t,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil

}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
	// return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(true)
	return NewStandAloneStorageReader(s, txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// txn := s.engine.Kv.NewTransaction(true)
	var wb engine_util.WriteBatch
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
	}
	s.engine.WriteKV(&wb)
	return nil
}
