package kafka_monitor

import (
	"encoding/hex"
	"log"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
)

var kafkaVersions = map[string]sarama.KafkaVersion{
	"":         sarama.V0_10_2_0,
	"0.8.0":    sarama.V0_8_2_0,
	"0.8.1":    sarama.V0_8_2_1,
	"0.8.2":    sarama.V0_8_2_2,
	"0.8":      sarama.V0_8_2_0,
	"0.9.0.0":  sarama.V0_9_0_0,
	"0.9.0.1":  sarama.V0_9_0_1,
	"0.9.0":    sarama.V0_9_0_0,
	"0.9":      sarama.V0_9_0_0,
	"0.10.0.0": sarama.V0_10_0_0,
	"0.10.0.1": sarama.V0_10_0_1,
	"0.10.0":   sarama.V0_10_0_0,
	"0.10.1.0": sarama.V0_10_1_0,
	"0.10.1":   sarama.V0_10_1_0,
	"0.10.2.0": sarama.V0_10_2_0,
	"0.10.2.1": sarama.V0_10_2_0,
	"0.10.2":   sarama.V0_10_2_0,
	"0.10":     sarama.V0_10_0_0,
	"0.11.0.1": sarama.V0_11_0_0,
	"0.11.0.2": sarama.V0_11_0_0,
	"0.11.0":   sarama.V0_11_0_0,
	"1.0.0":    sarama.V1_0_0_0,
	"1.1.0":    sarama.V1_1_0_0,
	"1.1.1":    sarama.V1_1_0_0,
	"2.0.0":    sarama.V2_0_0_0,
	"2.0.1":    sarama.V2_0_0_0,
	// "2.1.0":    sarama.V2_1_0_0,
	// "2.2.0":    sarama.V2_2_0_0,
	// "2.2.1":    sarama.V2_2_0_0,
	// "2.3.0":    sarama.V2_3_0_0,
}

func parseKafkaVersion(kafkaVersion string) sarama.KafkaVersion {
	version, ok := kafkaVersions[string(kafkaVersion)]
	if !ok {
		log.Printf("[kafka_monitor] invalid kafka version %s", kafkaVersion)
		return sarama.V1_1_0_0
	}
	return version
}

func (km *KafkaMonitor) getConfig() (*sarama.Config, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = km.ClientID
	if saramaConfig.ClientID == "" {
		saramaConfig.ClientID = generateKafkaClientID()
	}
	saramaConfig.Version = parseKafkaVersion(km.Version)
	saramaConfig.Consumer.Return.Errors = true

	tlsConfig, err := km.ClientConfig.TLSConfig()
	if err != nil {
		log.Printf("[kafka_monitor] fail to get tls config : %s", err)
		return nil, err
	}
	saramaConfig.Net.TLS.Config = tlsConfig

	if km.SaslEnable {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Handshake = km.SaslHandshake
		saramaConfig.Net.SASL.User = km.SaslUser
		saramaConfig.Net.SASL.Password = km.SaslPassword
	}
	return saramaConfig, nil
}

func generateKafkaClientID() string {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, 4)
	rand.Read(buf)
	return "telegraf-kafka-monitor-x" + hex.EncodeToString(buf)
}
