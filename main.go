package main

import (
	"flag"
	"log"

	"github.com/abbit/diskv/config"
	"github.com/abbit/diskv/db"
	"github.com/abbit/diskv/server"
)

var (
	configfile = flag.String("config", "config.yml", "path to config file")
	name       = flag.String("name", "", "name of this server")
)

func parseFlags() {
	flag.Parse()

	if *name == "" {
		log.Fatalf("Must provide name")
	}
}

func main() {
	parseFlags()

	config, err := config.New(*configfile, *name)
	if err != nil {
		log.Fatal(err)
	}

	db := db.New()
	server := server.New(db, config)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
