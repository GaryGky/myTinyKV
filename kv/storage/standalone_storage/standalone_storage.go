package standalone_storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
	conf   *config.Config
}

// NewStandAloneStorage 初始化一个数据库实例: In-memory True
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	err := os.MkdirAll(kvPath, os.ModePerm)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(raftPath, os.ModePerm)
	if err != nil {
		panic(err)
	}

	db := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)

	engine := engine_util.NewEngines(db, raftDB, kvPath, raftPath)

	return &StandAloneStorage{engine: engine, conf: conf}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneReader{
		txn: s.engine.Kv.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var err error

	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			err = engine_util.PutCF(s.engine.Kv, put.Cf, put.Key, put.Value)
		case storage.Delete:
			del := modify.Data.(storage.Delete)
			err = engine_util.DeleteCF(s.engine.Kv, del.Cf, del.Key)
		}

		if err != nil {
			fmt.Printf("StandAloneStorage Write Failed, %v", err)
			return err
		}
	}

	return err
}

type StandAloneReader struct {
	txn *badger.Txn
}

// GetCF 通过CF 和 Key 获取Value
func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var err error

	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)

	if err != nil && err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return val, err
}

// IterCF 把所有cf 开头的Key都Get出来
func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneReader) Close() {
	s.txn.Discard()
}

type StandAloneIterator struct {
	iter *engine_util.BadgerIterator
}

func (s StandAloneIterator) Item() engine_util.DBItem {
	return s.iter.Item()
}

func (s StandAloneIterator) Valid() bool {
	return s.iter.Valid()
}

func (s StandAloneIterator) Next() {
	s.iter.Next()
}

func (s StandAloneIterator) Seek(key []byte) {
	s.iter.Seek(key)
}

func (s StandAloneIterator) Close() {
	s.iter.Close()
}
