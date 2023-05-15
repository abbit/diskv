package service

import (
	"errors"
	"log"
	"sync"
)

var (
	NoNewLogEntriesError = errors.New("no new log entries")
)

type Service struct {
	storage      map[string][]byte
	storageMutex sync.Mutex

	log      []LogEntry
	logMutex sync.Mutex
}

type LogEntry struct {
	Index int
	Key   string
	Value []byte
}

type PutArgs struct {
	Key   string
	Value []byte
}

func New() *Service {
	return &Service{
		storage: make(map[string][]byte),
		log:     make([]LogEntry, 0),
	}
}

func (s *Service) Get(key string, reply *[]byte) error {
	s.storageMutex.Lock()
	defer s.storageMutex.Unlock()

	*reply = s.storage[key]
	return nil
}

func (s *Service) Put(args *PutArgs, reply *struct{}) error {
	s.storageMutex.Lock()
	defer s.storageMutex.Unlock()
	s.logMutex.Lock()
	defer s.logMutex.Unlock()

	s.storage[args.Key] = args.Value

	logEntry := LogEntry{
		Index: len(s.log),
		Key:   args.Key,
		Value: args.Value,
	}
	s.log = append(s.log, logEntry)
	log.Printf("Added log entry: %+v\n", logEntry)

	return nil
}

func (s *Service) GetNextLogEntry(index int, reply *LogEntry) error {
	s.logMutex.Lock()
	defer s.logMutex.Unlock()

	if index+1 >= len(s.log) {
		return NoNewLogEntriesError
	}
	*reply = s.log[index+1]
	return nil
}

func (s *Service) GetLastLogEntry() LogEntry {
	s.logMutex.Lock()
	defer s.logMutex.Unlock()

	if len(s.log) == 0 {
		return LogEntry{Index: -1}
	}
	return s.log[len(s.log)-1]
}
