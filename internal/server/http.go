package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// create a new instance an http server with handlers
func NewHTTPServer(addr string) *http.Server {
	httpSrv := newHTTPServer()
	router := mux.NewRouter()

	// route definitions for producer and consumer
	router.HandleFunc("/", httpSrv.handleProduce).Methods("POST")
	router.HandleFunc("/{offset:[0-9]+}", httpSrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}

// internal http server for the log
type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{Log: NewLog()}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}
type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	// unmarshal request
	var body ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// produce log
	offset, err := s.Log.Append(body.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// return offset as response
	res := ProduceResponse{Offset: offset}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	// extract offset from request path
	vars := mux.Vars(r)
	offset, err := strconv.ParseUint(vars["offset"], 10, 64)
	if err != nil {
		http.Error(w, "offset should be a positive integer", http.StatusUnprocessableEntity)
		return
	}

	// read record value from log
	record, err := s.Log.Read(offset)
	if errors.Is(err, ErrOffsetNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
