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
	memReader, err := storage.NewMemStorage().Reader(ctx)
	return memReader, err
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := &engine_util.WriteBatch{}
	for _, modify := range batch {
		wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	err := s.engine.WriteKV(wb)
	if err != nil {
		log.Fatalf("StandAlone.Write, error: %v\n", err)
	}
	return err
}
