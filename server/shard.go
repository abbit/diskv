package server

import (
	"github.com/abbit/diskv/db"
)

type ShardService struct {
	db *db.DB
}

type ShardServicePutArgs struct {
	key string
    value []byte
}

func (s *ShardService) Get(key string, reply *[]byte) error {
	*reply = s.db.Get(key)
	return nil
}

func (s *ShardService) Put(args *ShardServicePutArgs, reply *struct{}) error {
	s.db.Put(args.key, args.value)
	return nil
}
