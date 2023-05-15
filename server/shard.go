package server

import (
	"fmt"

	"github.com/abbit/diskv/db"
)

type ShardService struct {
	db      *db.DB
	replica bool
}

type ShardServicePutArgs struct {
	key   string
	value []byte
}

func NewShardService(db *db.DB, replica bool) *ShardService {
	return &ShardService{
		db:      db,
		replica: replica,
	}
}

func (s *ShardService) Get(key string, reply *[]byte) error {
	*reply = s.db.Get(key)
	return nil
}

func (s *ShardService) Put(args *ShardServicePutArgs, reply *struct{}) error {
	if s.replica {
		return fmt.Errorf("shard is readonly")
	}

	s.db.Put(args.key, args.value, true)
	return nil
}
