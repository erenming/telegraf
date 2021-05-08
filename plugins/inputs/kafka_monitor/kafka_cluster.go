package kafka_monitor

import (
	"fmt"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type ClusterNotify interface {
	OnDeleteTopic(topic string)
	OnBrokerOffsetUpdate(topic string, partitions int, partition int32, offset int64)
}

type KafkaCluster struct {
	client          sarama.Client
	topicPartitions map[string][]int32
	notify          ClusterNotify
	fetchMetadata   bool
}

func (kc *KafkaCluster) fetchTopicPartitions(client sarama.Client) error {
	// Get the current list of topics and make a map
	topicList, err := client.Topics()
	if err != nil {
		err = fmt.Errorf("failed to fetch topic list: %s", err)
		log.Println("[kafka_cluster] ", err)
		return err
	}
	// We'll use topicPartitions later
	topicPartitions := make(map[string][]int32)
	for _, topic := range topicList {
		partitions, err := client.Partitions(topic)
		if err != nil {
			err = fmt.Errorf("failed to fetch topic %s partition list: %s", topic, err)
			log.Println("[kafka_cluster] ", err)
			return err
		}
		topicPartitions[topic] = make([]int32, 0, len(partitions))
		for _, partitionID := range partitions {
			if _, err := client.Leader(topic, partitionID); err != nil {
				err = fmt.Errorf("failed to fetch leader for topic %s partition %d: %s", topic, partitionID, err)
				log.Println("[kafka_cluster] ", err)
				continue
			}
			topicPartitions[topic] = append(topicPartitions[topic], partitionID)
		}
	}
	// Check for deleted topics if we have a previous map to check against
	if kc.topicPartitions != nil {
		for topic := range kc.topicPartitions {
			if _, ok := topicPartitions[topic]; !ok {
				// Topic no longer exists
				kc.notify.OnDeleteTopic(topic)
			}
		}
	}
	// Save the new topicPartitions for next time
	kc.topicPartitions = topicPartitions
	return nil
}

func (kc *KafkaCluster) FetchOffsets(force bool) {
	if force || kc.fetchMetadata {
		kc.fetchMetadata = false
		kc.fetchTopicPartitions(kc.client)
	}
	if kc.topicPartitions == nil || len(kc.topicPartitions) <= 0 {
		return
	}
	requests, brokers := kc.generateOffsetRequests(kc.client)

	// Send out the OffsetRequest to each broker for all the partitions it is leader
	var wg = sync.WaitGroup{}
	var errorTopics = sync.Map{}
	for brokerID, request := range requests {
		wg.Add(1)
		go kc.getBrokerOffsets(&wg, &errorTopics, brokers[brokerID], request)
	}
	wg.Wait()
	// If there are any topics that had errors, force a metadata refresh on the next run
	errorTopics.Range(func(key, value interface{}) bool {
		kc.fetchMetadata = true
		return false
	})
}

func (kc *KafkaCluster) generateOffsetRequests(client sarama.Client) (map[int32]*sarama.OffsetRequest, map[int32]*sarama.Broker) {
	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)
	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range kc.topicPartitions {
		for _, partitionID := range partitions {
			broker, err := client.Leader(topic, partitionID)
			if err != nil {
				log.Printf("[kafka_cluster] failed to fetch leader for topic %s partition %d: %s", topic, partitionID, err)
				kc.fetchMetadata = true
				continue
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, partitionID, sarama.OffsetNewest, 1)
		}
	}
	return requests, brokers
}

func (kc *KafkaCluster) getBrokerOffsets(wg *sync.WaitGroup, errorTopics *sync.Map, broker *sarama.Broker, request *sarama.OffsetRequest) {
	defer wg.Done()
	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		log.Printf("[kafka_cluster] failed to fetch offsets from broker %d: %s", broker.ID(), err)
		broker.Close()
		return
	}
	// broker.Close()
	for topic, partitions := range response.Blocks {
		for partition, offsetResponse := range partitions {
			if offsetResponse.Err != sarama.ErrNoError {
				log.Printf("[kafka_cluster] error in OffsetResponse, broker: %d, topic: %s, partition: %d, error: %s",
					broker.ID(), topic, partition, offsetResponse.Err.Error())
				// Gather a list of topics that had errors
				errorTopics.Store(topic, true)
				continue
			}
			kc.notify.OnBrokerOffsetUpdate(topic, cap(kc.topicPartitions[topic]), partition, offsetResponse.Offsets[0])
		}
	}
}
