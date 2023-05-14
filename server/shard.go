package server

import (
	"fmt"

	"github.com/abbit/diskv/db"
)

type ShardService struct {
	db *db.DB
}

type ShardServicePutArgs struct {
	key, value []byte
}

func (s *ShardService) Get(key []byte, reply *[]byte) error {
	value, err := s.db.Get(key)
	if err != nil {
		return fmt.Errorf("error with get from db: %v", err)
	}
	*reply = value
	return nil
}

func (s *ShardService) Put(args *ShardServicePutArgs, reply *struct{}) error {
	err := s.db.Put(args.key, args.value)
	if err != nil {
		return fmt.Errorf("error with put to db: %v", err)
	}
	return nil
}
