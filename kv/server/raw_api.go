package server

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if val == nil || (err != nil && err == badger.ErrKeyNotFound) {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}

	return &kvrpcpb.RawGetResponse{
		Value: val,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	modify := storage.Modify{Data: put}
	err := server.storage.Write(&kvrpcpb.Context{}, []storage.Modify{modify})
	if err != nil {
		log.Fatal("Server.RawPut Failed: ", err)
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify := storage.Modify{Data: delete}
	err := server.storage.Write(&kvrpcpb.Context{}, []storage.Modify{modify})
	if err != nil {
		log.Fatal("Server.RawDelete Failed ", err)
	}
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		log.Fatal("Server RawScan Create Reader Failed, ", err)
		return nil, err
	}

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	resp := make([]*kvrpcpb.KvPair, 0)
	cnt := uint32(0)
	for iter.Seek([]byte(req.Cf)); iter.Valid() && cnt < req.Limit; iter.Next() {
		value, err := iter.Item().Value()
		if err != nil {
			log.Fatal("Server RawScan Scan CF Failed, ", err)
			continue
		}
		resp = append(resp, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		})
		cnt++
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: resp,
	}, nil
}
