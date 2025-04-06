package main

import (
	"log"

	"github.com/mrshabel/gumlog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8000")
	log.Fatal(srv.ListenAndServe())
}
