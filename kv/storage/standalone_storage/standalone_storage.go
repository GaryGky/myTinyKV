package standalone_storage

import (
	"fmt"
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
		inner:     s,
		iterCount: 0,
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
		}
	}

	return err
}

type StandAloneReader struct {
	inner     *StandAloneStorage
	iterCount int
}

// GetCF 通过CF 和 Key 获取Value
func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var err error

	val, err := engine_util.GetCF(s.inner.engine.Kv, cf, key)
	if err != nil {
		return nil, err
	}

	return val, nil
}

//
func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	//TODO implement me
	panic("implement me")
}

func (s *StandAloneReader) Close() {
	//TODO implement me
	panic("implement me")
}

type StandAloneIterator struct {
	reader *StandAloneReader
	item   *StandAloneItem
}

func (s StandAloneIterator) Item() engine_util.DBItem {
	//TODO implement me
	panic("implement me")
}

func (s StandAloneIterator) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (s StandAloneIterator) Next() {
	//TODO implement me
	panic("implement me")
}

func (s StandAloneIterator) Seek(bytes []byte) {
	//TODO implement me
	panic("implement me")
}

func (s StandAloneIterator) Close() {
	//TODO implement me
	panic("implement me")
}

type StandAloneItem struct {
	key   []byte
	value []byte
}

func (s StandAloneItem) Key() []byte {
	//TODO implement me
	panic("implement me")
}

func (s StandAloneItem) KeyCopy(dst []byte) []byte {
	//TODO implement me
	panic("implement me")
}

func (s StandAloneItem) Value() ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (s StandAloneItem) ValueSize() int {
	//TODO implement me
	panic("implement me")
}

func (s StandAloneItem) ValueCopy(dst []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}
