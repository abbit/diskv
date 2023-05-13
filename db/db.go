package db

import (
	"fmt"

	"github.com/abbit/diskv/config"
	bolt "go.etcd.io/bbolt"
)

var (
	defaultBucketName = []byte("default")
)

type DB struct {
	bolt *bolt.DB
}

func New(config *config.Config) (*DB, error) {
	dbfile := fmt.Sprint(config.ThisShard().Name, ".db")
	boltdb, err := bolt.Open(dbfile, 0666, nil)
	if err != nil {
		return nil, err
	}

	boltdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucketName)
		if err != nil {
			return err
		}
		return nil
	})

	return &DB{boltdb}, nil
}

func (db *DB) Close() error {
	return db.bolt.Close()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	var value []byte

	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucketName)
		value = b.Get([]byte(key))
		return nil
	})
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (db *DB) Put(key, value []byte) error {
	err := db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucketName)
		return b.Put(key, value)
	})
	if err != nil {
		return err
	}

	return nil
}
