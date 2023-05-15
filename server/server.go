package server

import (
	"io"
	"log"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/abbit/diskv/config"
	"github.com/abbit/diskv/service"
)

var (
	ShardHeaderName = "X-From-Shard"
)

type Server struct {
	config *config.Config

	service *service.Service

	nodeClientMap map[string]*rpc.Client
	mapMutex      sync.Mutex

	rpc  *rpc.Server
	http *http.Server
}

func New(config *config.Config) *Server {
	server := &Server{
		config:  config,
		service: service.New(),
		http: &http.Server{
			Addr: config.ThisNode().HttpAddress(),
		},
		rpc:           rpc.NewServer(),
		nodeClientMap: make(map[string]*rpc.Client),
	}

	server.rpc.Register(server.service)
	mux := http.NewServeMux()
	mux.HandleFunc(rpc.DefaultRPCPath, server.rpc.ServeHTTP)
	mux.HandleFunc("/", server.routingHandler)
	server.http.Handler = logMiddleware(mux)

	return server
}

func (s *Server) ListenAndServe() error {
	if s.config.ThisNode().Replica {
		log.Println("Starting replication loop")
		go s.replicationLoop()
	}

	log.Printf("HTTP server listening on %s\n", s.http.Addr)
	return s.http.ListenAndServe()
}

func (s *Server) replicationLoop() {
	master := s.config.GetShardMasterNode(s.config.ThisNode().Index)
	masterClient, err := s.getNodeClient(master)
	if err != nil {
		log.Fatalf("Can't connect to shard master: %v\n", err)
	}

	for {
		lastLogEntry := s.service.GetLastLogEntry()
		var newLogEntry service.LogEntry
		err := masterClient.Call("Service.GetNextLogEntry", lastLogEntry.Index, &newLogEntry)
		if err == nil {
			s.service.Put(&service.PutArgs{Key: newLogEntry.Key, Value: newLogEntry.Value}, nil)
		} else if err.Error() == service.NoNewLogEntriesError.Error() {
			// no new log entries
		} else {
			log.Println("Error:", err)
		}

		time.Sleep(250 * time.Millisecond)
	}
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
	key := r.URL.Path[1:]
	nodesForKey := s.config.GetNodesForKey(key)

	var value []byte
	var respondedNode *config.Node
	if s.config.ThisNode().Index == nodesForKey[0].Index {
		err := s.service.Get(key, &value)
		if err != nil {
			log.Println("Error:", err)
			http.Error(w, "error with get from db", http.StatusInternalServerError)
			return
		}
		respondedNode = s.config.ThisNode()
	} else {
		for _, nodeForKey := range nodesForKey {
			client, err := s.getNodeClient(nodeForKey)
			if err != nil {
				continue
			}
			err = client.Call("Service.Get", key, &value)
			if err != nil {
				continue
			}
			respondedNode = nodeForKey
			break
		}
	}

	if respondedNode == nil {
		http.Error(w, "error all shard's nodes", http.StatusInternalServerError)
		return
	}

	w.Header().Set(ShardHeaderName, respondedNode.Name)
	w.Write(value)
}

func (s *Server) putHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[1:]

	if s.config.ThisNode().Replica {
		http.Redirect(w, r, "http://"+s.config.GetMasterNodeForKey(key).HttpAddress()+"/"+key, http.StatusPermanentRedirect)
		return
	}

	nodeForKey := s.config.GetMasterNodeForKey(key)
	value, err := io.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Body.Close()

	args := &service.PutArgs{Key: key, Value: value}
	if s.config.ThisNode().Index == nodeForKey.Index {
		err := s.service.Put(args, nil)
		if err != nil {
			log.Println("Error:", err)
			http.Error(w, "error with put to db", http.StatusInternalServerError)
			return
		}
	} else {
		client, err := s.getNodeClient(nodeForKey)
		if err != nil {
			log.Println("Error:", err)
			http.Error(w, "error with dialing shard", http.StatusInternalServerError)
			return
		}
		err = client.Call("Service.Put", args, nil)
		if err != nil {
			log.Println("Error:", err)
			http.Error(w, "error with put on different shard", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set(ShardHeaderName, nodeForKey.Name)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getNodeClient(node *config.Node) (*rpc.Client, error) {
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	if client, ok := s.nodeClientMap[node.Name]; ok {
		return client, nil
	}

	client, err := rpc.DialHTTP("tcp", node.HttpAddress())
	if err != nil {
		return nil, err
	}

	s.nodeClientMap[node.Name] = client
	return client, nil
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s\n", r.RemoteAddr, r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}
