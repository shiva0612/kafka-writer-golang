package kafkaProducer

import "github.com/segmentio/kafka-go"

type KafkaProducerConn struct {
	Enable                 bool     `json:"Enable"`
	Address                []string `json:"Address"`
	Balancer               int      `json:"Balancer"`
	MaxAttempts            int      `json:"MaxAttempts"`
	BatchSize              int      `json:"BatchSize"`
	BatchBytes             int64    `json:"BatchBytes"`
	BatchTimeout           int      `json:"BatchTimeout"`
	ReadTimeout            int      `json:"ReadTimeout"`
	WriteTimeout           int      `json:"WriteTimeout"`
	RequiredAcks           int      `json:"RequiredAcks"`
	Async                  bool     `json:"Async"`
	Compression            int      `json:"Compression"`
	Logger                 string   `json:"Logger"`
	ErrorLogger            string   `json:"ErrorLogger"`
	AllowAutoTopicCreation bool     `json:"AllowAutoTopicCreation"`
	DialTimeout            int      `json:"DialTimeout"`
	IdleTimeout            int      `json:"IdleTimeout"`
	MetadataTTL            int      `json:"MetadataTTL"`
	ClientID               string   `json:"ClientID"`

	//Dialer tls config
	TLS    bool `json:"TLS"`
	TLSCfg struct {
		InSecureSkipVerify bool   `json:"InSecureSkipVerify"`
		ServerName         string `json:"ServerName"`
		Public             string `json:"Public"`
		Private            string `json:"Private"`
		RootCAs            string `json:"RootCAs"`
	} `json:"TLSCfg"`
	WriteTransactionTimeout int `json:"WriteTransactionTimeout"`
	kafkaTransport          *kafka.Transport
}

func (cfg *KafkaProducerConn) SetKafkaTransport(transport *kafka.Transport) {
	cfg.kafkaTransport = transport
}
func (cfg *KafkaProducerConn) GetKafkaTransport() *kafka.Transport {
	return cfg.kafkaTransport
}
