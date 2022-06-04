package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"log"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
}

// NewStandAloneStorage 初始化一个数据库实例: In-memory True
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db, err := badger.Open(badger.Options{
		Dir:      conf.StoreAddr,
		ValueDir: conf.DBPath,
	})
	if err != nil {
		log.Fatal(err)
	}

	engine := engine_util.NewEngines(db, nil, conf.DBPath, "")

	return &StandAloneStorage{engine: engine}
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
	wb := &engine_util.WriteBatch{}
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
		case storage.Delete:
			wb.DeleteMeta(modify.Key())
		}
	}
	err := s.engine.WriteKV(wb)
	return err
}

type StandAloneReader struct {
	inner     *StandAloneStorage
	iterCount int
}

// GetCF 通过CF 和 Key 获取Value
func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

// 找到
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
