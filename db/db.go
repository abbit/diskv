package db

type logEntry struct {
	index int
	key   string
	value []byte
}

type DB struct {
	storage map[string][]byte
	log     []logEntry
}

func New() *DB {
	return &DB{
		storage: make(map[string][]byte),
		log:     make([]logEntry, 0),
	}
}

func (db *DB) Get(key string) []byte {
	return db.storage[key]
}

func (db *DB) Put(key string, value []byte, log bool) {
	db.storage[key] = value

	if log {
		logEntry := logEntry{
			index: len(db.log),
			key:   key,
			value: value,
		}
		db.log = append(db.log, logEntry)
	}
}

func (db *DB) GetLog() {
}
