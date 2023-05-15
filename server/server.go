package server

import (
	"io"
	"log"
	"net/http"
	"net/rpc"

	"github.com/abbit/diskv/config"
	"github.com/abbit/diskv/db"
)

var (
	ShardHeaderName = "X-From-Shard"
)

type Server struct {
	config *config.Config

	shardService *ShardService

	rpc  *rpc.Server
	http *http.Server

	shardClientMap map[int]*rpc.Client // TODO: protect with mutex?
}

func New(db *db.DB, config *config.Config) *Server {
	server := &Server{
		config:       config,
		shardService: NewShardService(db),
		http: &http.Server{
			Addr: config.ThisShard().HttpAddress(),
		},
		rpc:            rpc.NewServer(),
		shardClientMap: make(map[int]*rpc.Client),
	}

	server.rpc.Register(server.shardService)
	mux := http.NewServeMux()
	mux.HandleFunc(rpc.DefaultRPCPath, server.rpc.ServeHTTP)
	mux.HandleFunc("/", server.routingHandler)
	server.http.Handler = logMiddleware(mux)

	return server
}

func (s *Server) ListenAndServe() error {
	log.Printf("HTTP server listening on %s\n", s.http.Addr)
	return s.http.ListenAndServe()
}

func (s *Server) Close() error {
	for _, client := range s.shardClientMap {
		client.Close()
	}
	return s.http.Close()
}

func (s *Server) routingHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.getHandler(w, r)
	case "PUT":
		s.putHandler(w, r)
	default:
		http.Error(w, "Wrong method", http.StatusBadRequest)
	}
}

func (s *Server) getHandler(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.URL.Path[1:])
	shardForKey := s.config.GetShardForKey(key)

	var value []byte
	if s.config.ThisShard().Index == shardForKey.Index {
		err := s.shardService.Get(key, &value)
		if err != nil {
			http.Error(w, "error with get from db", http.StatusInternalServerError)
			return
		}
	} else {
		// call rpc method on correct shard
		client, err := s.getShardClient(shardForKey)
		if err != nil {
			http.Error(w, "error with dialing shard", http.StatusInternalServerError)
			return
		}
		err = client.Call("ShardService.Get", key, &value)
		if err != nil {
			http.Error(w, "error with get from db", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set(ShardHeaderName, shardForKey.Name)
	w.Write(value)
}

func (s *Server) putHandler(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.URL.Path[1:])
	shardForKey := s.config.GetShardForKey(key)
	value, err := io.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Body.Close()

	args := &ShardServicePutArgs{key, value}
	if s.config.ThisShard().Index == shardForKey.Index {
		err := s.shardService.Put(args, nil)
		if err != nil {
			http.Error(w, "error with put to db", http.StatusInternalServerError)
			return
		}
	} else {
		client, err := s.getShardClient(shardForKey)
		if err != nil {
			http.Error(w, "error with dialing shard", http.StatusInternalServerError)
			return
		}
		// call rpc method on correct shard
		err = client.Call("ShardService.Set", args, nil)
		if err != nil {
			http.Error(w, "error with get from db", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set(ShardHeaderName, shardForKey.Name)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getShardClient(shard *config.Shard) (*rpc.Client, error) {
	client, ok := s.shardClientMap[shard.Index]
	if !ok {
		rpcClient, err := rpc.DialHTTPPath("tcp", shard.HttpAddress(), rpc.DefaultRPCPath)
		if err != nil {
			return nil, err
		}
		s.shardClientMap[shard.Index] = rpcClient
		client = rpcClient
	}
	return client, nil
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s\n", r.RemoteAddr, r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}
