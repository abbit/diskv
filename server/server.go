package server

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/abbit/diskv/config"
	"github.com/abbit/diskv/db"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

var (
    ShardHeaderName = "X-From-Shard"
)

type Server struct {
	config *config.Config
	db     *db.DB
	http   *http.Server
}

func New(db *db.DB, config *config.Config) *Server {
	server := &Server{
		config: config,
		db:     db,
		http: &http.Server{
			Addr: config.ThisShard().Address,
		},
	}

	r := chi.NewRouter()
    r.Use(middleware.Logger)
	r.Get("/store/{key}", server.routingHandler)
	r.Put("/store/{key}", server.routingHandler)

	server.http.Handler = r
	return server
}

func (s *Server) ListenAndServe() error {
	fmt.Printf("listening on %s\n", s.http.Addr)
	return s.http.ListenAndServe()
}

func (s *Server) routingHandler(w http.ResponseWriter, r *http.Request) {
	key := getKeyFromParams(r)
	shardForKey := s.config.GetShardForKey(key)

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
	key := getKeyFromParams(r)
	shardForKey := s.config.GetShardForKey(key)

    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "error reading body", http.StatusInternalServerError)
        return
    }
    defer r.Body.Close()

    req, err := http.NewRequest(r.Method, fmt.Sprintf("http://%s/store/%s", shardForKey.Address, key), bytes.NewReader(body))
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
    w.WriteHeader(resp.StatusCode)
}

func (s *Server) getHandler(w http.ResponseWriter, r *http.Request) {
	key := getKeyFromParams(r)
	value, err := s.db.Get(key)
	if err != nil {
		http.Error(w, "error with get from db", http.StatusInternalServerError)
		return
	}

	w.Header().Set(ShardHeaderName, s.config.ThisShard().Name)
	w.Write(value)
}

func (s *Server) putHandler(w http.ResponseWriter, r *http.Request) {
	key := getKeyFromParams(r)
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

func getKeyFromParams(r *http.Request) []byte {
	return []byte(chi.URLParam(r, "key"))
}
