package prom

import (
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	reg = prometheus.NewRegistry()
)

var (
	ToBePushed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "to_be_pushed_to",
		Help: "Counter to track messages to be written to kafka topics",
	}, []string{"topic"})
	Pushed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pushed",
		Help: "Counter to track messages written to kafka topics",
	}, []string{"topic"})
	ErrPushing = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "error_pushing",
		Help: "Counter to track messages that are failed while writing to kakfa",
	}, []string{"topic"})
)

// HTTP server to expose Prometheus metrics
func HttpServerHandle() {

	reg.MustRegister(ToBePushed, Pushed, ErrPushing)

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "ok")
	})

	go func() {
		log.Println("starting prometheus")
		log.Fatal(http.ListenAndServe(":8081", nil))
	}()
}
