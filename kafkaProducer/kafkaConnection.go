package kafkaProducer

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	KAFKA_LOGGER = "KAFKA_PRODUCER_LOGGER"
)

type CustomBal struct {
}

func (cb *CustomBal) Balance(msg kafka.Message, parts ...int) int {
	i := bytes.LastIndexByte(msg.Key, byte(','))
	subID, _ := strconv.Atoi(string(msg.Key[i+1:]))
	//log.Println("publishing to partition: ", subID%len(parts))
	return subID % len(parts)
}

func CheckKafkaConn(cnf *KafkaProducerConn) error {
	var conn *kafka.Conn
	var err error
	dialer := &kafka.Dialer{Timeout: getDurInSec(cnf.DialTimeout), TLS: cnf.GetKafkaTransport().TLS}
	for _, ip := range cnf.Address {
		conn, err = dialer.DialContext(context.Background(), "tcp", ip)
		if err == nil {
			break
		}
	}
	if conn == nil {
		return fmt.Errorf("kafka is not reachable: %s", err.Error())
	}
	conn.Close()
	return nil
}

func GetKafkaTLS(cnf *KafkaProducerConn) (*tls.Config, error) {
	var err error
	tlsConf := &tls.Config{
		InsecureSkipVerify: cnf.TLSCfg.InSecureSkipVerify,
	}
	if cnf.TLSCfg.ServerName != "" {
		tlsConf.ServerName = cnf.TLSCfg.ServerName
	}
	if cnf.TLSCfg.RootCAs != "" {
		tlsConf.RootCAs, err = GetCAcrt(cnf.TLSCfg.RootCAs)
		if err != nil {
			return nil, err
		}
	}
	if cnf.TLSCfg.Public != "" && cnf.TLSCfg.Private != "" {
		tlsConf.Certificates, err = GetCerts(cnf.TLSCfg.Public, cnf.TLSCfg.Private)
		if err != nil {
			return nil, err
		}
	}
	return tlsConf, err
}

func ConfigureKafkaTransport(cnf *KafkaProducerConn) *kafka.Transport {

	if cnf.GetKafkaTransport() == nil {
		var transCfg = &kafka.Transport{
			DialTimeout: getDurInSec(cnf.DialTimeout),
			IdleTimeout: getDurInSec(cnf.IdleTimeout),
			MetadataTTL: getDurInSec(cnf.MetadataTTL),
			ClientID:    cnf.ClientID,
		}
		cnf.SetKafkaTransport(transCfg)
	}
	return cnf.GetKafkaTransport()
}

func GetKafkaConn(cnf *KafkaProducerConn) *kafka.Writer {
	kw := &kafka.Writer{
		Addr:                   kafka.TCP(cnf.Address...),
		Async:                  cnf.Async,
		AllowAutoTopicCreation: cnf.AllowAutoTopicCreation,
		Balancer:               getBalancer(cnf.Balancer),
		RequiredAcks:           getRequiredAcks(cnf.RequiredAcks),
	}

	if cnf.MaxAttempts > 0 {
		kw.MaxAttempts = cnf.MaxAttempts
	}
	if cnf.Logger != "" {
		kw.Logger = getLogger(cnf.Logger)
	}
	if cnf.ErrorLogger != "" {
		kw.ErrorLogger = getLogger(cnf.ErrorLogger)
	}
	if cnf.BatchSize > 0 {
		kw.BatchSize = cnf.BatchSize
	}
	if cnf.BatchBytes > 0 {
		kw.BatchBytes = cnf.BatchBytes
	}
	if cnf.BatchTimeout > 0 {
		kw.BatchTimeout = getDurInSec(cnf.BatchTimeout)
	}
	if cnf.ReadTimeout > 0 {
		kw.ReadTimeout = getDurInSec(cnf.ReadTimeout)
	}
	if cnf.WriteTimeout > 0 {
		kw.WriteTimeout = getDurInSec(cnf.WriteTimeout)
	}
	if cnf.Compression > 0 {
		kw.Compression = getKafkaCompression(cnf.Compression)
	}
	return kw
}

func getBalancer(bal int) kafka.Balancer {
	switch bal {
	case 0:
		return &kafka.Hash{}
	case 1:
		return &CustomBal{}
	}
	return &kafka.Hash{}
}

func getRequiredAcks(acks int) kafka.RequiredAcks {
	switch acks {
	case 0:
		return kafka.RequireNone
	case 1:
		return kafka.RequireOne
	case -1:
		return kafka.RequireAll
	}
	return kafka.RequireNone
}

func getDurInSec(val int) time.Duration {
	return time.Duration(time.Duration(val) * time.Second)
}

func getLogger(val string) kafka.Logger {
	switch val {
	case "stdout":
		return &KafkaLogger{}
	case "stderr":
		return &KafkaLogger{}
	}
	return &KafkaLogger{}
}

type KafkaLogger struct {
}

func (l *KafkaLogger) Printf(msg string, val ...interface{}) {
	log.Printf(msg, val...)
}

func getKafkaCompression(val int) kafka.Compression {
	switch val {
	case 1:
		return kafka.Gzip
	case 2:
		return kafka.Snappy
	case 3:
		return kafka.Lz4
	case 4:
		return kafka.Zstd
	}
	return kafka.Gzip
}

func GetCAcrt(Cafile string) (*x509.CertPool, error) {
	caCert, err := ioutil.ReadFile(Cafile)
	if err != nil {
		msg := fmt.Sprintf("Error while loading CA certificate: %s", err.Error())
		log.Println(msg)
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return caCertPool, nil
}

func GetCerts(public string, private string) ([]tls.Certificate, error) {
	if public != "" && private != "" {
		cert, err := tls.LoadX509KeyPair(public, private)
		if err != nil {
			msg := fmt.Sprintf("Error while loading certificates: %s", err.Error())
			log.Println(msg)
			return nil, err
		}
		return []tls.Certificate{cert}, nil
	}
	msg := fmt.Sprint("public and private key pairs are not provided ")
	log.Println(msg)
	return nil, fmt.Errorf(msg)
}
