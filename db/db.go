package db

import (
	"fmt"

	"github.com/abbit/diskv/config"
	bolt "go.etcd.io/bbolt"
)

var (
	defaultBucketName = []byte("default")
	logBucketName = []byte("state-log")
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

    err = boltdb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucketName); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(logBucketName); err != nil {
			return err
		}
		return nil
	})
    if err != nil {
        return nil, err
    }

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

func (db *DB) Put(key, value []byte, log bool) error {
	err := db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucketName)
        if err := b.Put(key, value); err != nil {
            return err
        }

        if log {
            b = tx.Bucket(logBucketName)
            lastkey, _ := b.Cursor().Last()
            if err := b.Put(key, value); err != nil {
                return err
            }
        }

        return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) GetLog(key []byte) ([]byte, error) {
    var value []byte

    err := db.bolt.View(func(tx *bolt.Tx) error {
        b := tx.Bucket(logBucketName)
        value = b.Get([]byte(key))
        return nil
    })
    if err != nil {
        return nil, err
    }

    return value, nil
}
