package server

import (
	"bytes"
	"context"
	"fmt"
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
	db     *db.DB
	http   *http.Server
    rpc *rpc.Server
}

func New(db *db.DB, config *config.Config) *Server {
	server := &Server{
		config: config,
		db:     db,
		http: &http.Server{
			Addr: config.ThisShard().Address,
		},
        rpc: rpc.NewServer(),
	}

    mux := http.NewServeMux()
    mux.HandleFunc("/", server.routingHandler)
	server.http.Handler = logMiddleware(mux)

	return server
}

func (s *Server) ListenAndServe() error {
	fmt.Printf("listening on %s\n", s.http.Addr)
	return s.http.ListenAndServe()
}

func (s *Server) routingHandler(w http.ResponseWriter, r *http.Request) {
    key := []byte(r.URL.Path[1:])
	shardForKey := s.config.GetShardForKey(key)
    r = keyToContext(r, key)
    r = shardToContext(r, shardForKey)

	if s.config.ThisShard().Index != shardForKey.Index {
        s.redirectHandler(w, r)
		return
	}

    switch r.Method {
    case "GET":
        s.getHandler(w, r)
    case "PUT":
        s.putHandler(w, r)
    default:
        http.Error(w, "Wrong method", http.StatusBadRequest)
    }
}

func (s *Server) redirectHandler(w http.ResponseWriter, r *http.Request) {
    key := keyFromContext(r)
    shard := shardFromContext(r)

    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "error reading body", http.StatusInternalServerError)
        return
    }
    defer r.Body.Close()

    req, err := http.NewRequest(r.Method, fmt.Sprintf("http://%s/store/%s", shard.Address, key), bytes.NewReader(body))
    if err != nil {
        http.Error(w, "error creating request", http.StatusInternalServerError)
        return
    }

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        http.Error(w, "error sending request", http.StatusInternalServerError)
        return
    }
    defer resp.Body.Close()

    w.Header().Set(ShardHeaderName, resp.Header.Get(ShardHeaderName))
    if _, err := io.Copy(w, resp.Body); err != nil {
        http.Error(w, "error copying body", http.StatusInternalServerError)
        return
    }
}

func (s *Server) getHandler(w http.ResponseWriter, r *http.Request) {
    key := keyFromContext(r)

	value, err := s.db.Get(key)
	if err != nil {
		http.Error(w, "error with get from db", http.StatusInternalServerError)
		return
	}

	w.Header().Set(ShardHeaderName, s.config.ThisShard().Name)
	w.Write(value)
}

func (s *Server) putHandler(w http.ResponseWriter, r *http.Request) {
    key := keyFromContext(r)

	value, err := io.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Body.Close()

	err = s.db.Put(key, value)
	if err != nil {
		http.Error(w, "error with put to db", http.StatusInternalServerError)
		return
	}

	w.Header().Set(ShardHeaderName, s.config.ThisShard().Name)
	w.WriteHeader(http.StatusOK)
}

func keyToContext(r *http.Request, key []byte) *http.Request {
    return r.WithContext(context.WithValue(r.Context(), "key", key))
}

func keyFromContext(r *http.Request) []byte {
    return r.Context().Value("key").([]byte)
}

func shardToContext(r *http.Request, shard config.Shard) *http.Request {
    return r.WithContext(context.WithValue(r.Context(), "shard", shard))
}

func shardFromContext(r *http.Request) config.Shard {
    return r.Context().Value("shard").(config.Shard)
}

func logMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        log.Printf("%s %s %s\n", r.RemoteAddr, r.Method, r.URL.Path)
        next.ServeHTTP(w, r)
    })
}

