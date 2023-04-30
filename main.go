package main

import (
	"io"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	bolt "go.etcd.io/bbolt"
)

var (
    defaultBucketName = []byte("default")
)

func main() {
    db, err := bolt.Open("diskv.db", 0666, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    db.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists(defaultBucketName)
        if err != nil {
            return err
        }
        return nil
    })

    r := chi.NewRouter()
    r.Use(middleware.Logger)

    r.Get("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
       w.Write([]byte("ok")) 
    })

    r.Get("/store/{key}", func(w http.ResponseWriter, r *http.Request) {
        key := chi.URLParam(r, "key")
        db.View(func(tx *bolt.Tx) error {
            b := tx.Bucket(defaultBucketName)
            v := b.Get([]byte(key))
            w.Write(v)
            return nil
        })
    })

    r.Post("/store/{key}", func(w http.ResponseWriter, r *http.Request) {
        key := chi.URLParam(r, "key")
        value, err := io.ReadAll(r.Body)
        if err != nil {
            log.Fatal(err)
        }
        defer r.Body.Close()

        db.Update(func(tx *bolt.Tx) error {
            b := tx.Bucket(defaultBucketName)
            b.Put([]byte(key), value)
            return nil
        })
    })

    r.Delete("/store/{key}", func(w http.ResponseWriter, r *http.Request) {
        key := chi.URLParam(r, "key")
        db.Update(func(tx *bolt.Tx) error {
            b := tx.Bucket(defaultBucketName)
            b.Delete([]byte(key))
            return nil
        })
    })

    http.ListenAndServe(":3000", r)
}
