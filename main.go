package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	kp "jio-kw/kafkaProducer"
	"jio-kw/prom"

	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

var (
	KafkaConn *kafka.Writer
	wp        *workerPool
)

func main() {

	//set the config
	cfgPath := flag.String("config_path", "./configs/config.json", "config file path.")
	poolSize := flag.Int("pool_size", 10, "worker pool size.")
	flag.Parse()

	wp = NewWorkerPool(*poolSize)
	wp.Run()

	exCh := make(chan os.Signal, 3)
	signal.Notify(exCh, syscall.SIGINT)

	b, err := ioutil.ReadFile(*cfgPath)
	if err != nil {
		log.Fatalln("error reading the config file: ", err.Error())
	}
	kpConn := &kp.KafkaProducerConn{}
	err = json.Unmarshal(b, kpConn)
	if err != nil {
		log.Fatalln("error un-marshalling the config file: ", err.Error())
	}

	err = configureKafka(kpConn)
	if err != nil {
		log.Fatalln("failed to configure kafka: ", err.Error())
	}
	defer func() {
		err := KafkaConn.Close()
		if err != nil {
			log.Println("error closing kafka writers : ", err.Error())
		}
	}()

	prom.HttpServerHandle()

	//configure http router
	r := mux.NewRouter()
	r.HandleFunc("/push", PushHandler).Methods("POST")
	http.Handle("/", r)
	fmt.Println("Server is running on :8080...")
	go func() {
		http.ListenAndServe(":8080", nil)
	}()
	<-exCh
	close(exCh)
	close(wp.queuedTaskC)
	time.Sleep(3 * time.Second) //waiting for the queued task to complete
	log.Println("server closing...")
}

func PushHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic name is empty", http.StatusBadRequest)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is empty", http.StatusBadRequest)
		return
	}
	msg, err := io.ReadAll(r.Body)
	if string(msg) == "" {
		http.Error(w, "body is empty", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	prom.ToBePushed.WithLabelValues(topic).Inc()

	wp.AddTask(func() {
		err = kp.WriteToKafka(context.Background(), KafkaConn, topic, []byte(key), msg)
		if err != nil {
			log.Printf("failed to write to kafka : topic = %s, key = %s, msg = %s err = %s", topic, key, string(msg), err.Error())
			prom.ErrPushing.WithLabelValues(topic).Inc()

			http.Error(w, "failed in writing to kafka", http.StatusInternalServerError)
			return
		}
		prom.Pushed.WithLabelValues(topic).Inc()
	})

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Data written to Kafka successfully")
}

func configureKafka(kpConn *kp.KafkaProducerConn) error {
	KafkaConn = kp.GetKafkaConn(kpConn)
	kafkaTransport := kp.ConfigureKafkaTransport(kpConn)
	var err error
	if kpConn.TLS {
		kafkaTransport.TLS, err = kp.GetKafkaTLS(kpConn)
		if err != nil {
			msg := fmt.Sprintf("Could not connect with kafka: %s", err)
			log.Println(msg)
		}
	}
	KafkaConn.Transport = kafkaTransport

	if err := kp.CheckKafkaConn(kpConn); err != nil {
		return fmt.Errorf("error configuring kafka writer: %s", err.Error())
	}
	log.Println("kafka configured successfully")
	return nil
}
