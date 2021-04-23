package kafka_monitor

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type KafkaMonitor struct {
	Servers  string
	Version  string
	ClientID string

	tls.ClientConfig
	SaslEnable    bool
	SaslHandshake bool
	SaslUser      string
	SaslPassword  string

	OffsetsTopic          string
	StartLatest           bool
	BackfillEarliest      bool
	ReportedConsumerGroup string
	GroupWhitelist        string
	GroupBlacklist        string
	ExpireGroup           int64
	// TopicFilter           string

	initialized bool

	client           sarama.Client
	kafkaCluster     *KafkaCluster
	kafkaConsumer    *KafkaConsumer
	topicOffsets     map[string][]int64
	consumer         map[string]*consumerGroup
	topicOffsetsLock sync.RWMutex
	consumerLock     sync.RWMutex
	// topicFilter      *regexp.Regexp

	lastFetchMetadata int64

	lastConsumerStatus map[string]*consumerStatus
}

type consumerStatus struct {
	partitions int
	timestamp  int64
	offset     int64
	logsize    int64
}

type consumerPartition struct {
	offset     int64
	order      int64
	lastCommit int64
	owner      string // A string that describes the consumer host that currently owns this partition, if the information is available
	clientID   string
}

type consumerGroup struct {
	lock       sync.RWMutex
	topics     map[string][]*consumerPartition
	lastCommit int64
}

func NewKafkaMonitor() *KafkaMonitor {
	return &KafkaMonitor{
		Servers:  "",
		Version:  "1.1.0",
		ClientID: "",
		// ...
		SaslEnable:    false,
		SaslHandshake: true,
		SaslUser:      "",
		SaslPassword:  "",

		topicOffsets:       make(map[string][]int64),
		consumer:           make(map[string]*consumerGroup),
		lastConsumerStatus: make(map[string]*consumerStatus),

		OffsetsTopic:     "__consumer_offsets",
		StartLatest:      true,
		BackfillEarliest: true,
		ExpireGroup:      86400, //604800,
	}
}

// SampleConfig returns the default configuration of the Input
func (km *KafkaMonitor) SampleConfig() string {
	return ""
}

// Description returns a one-sentence description on the Input
func (km *KafkaMonitor) Description() string {
	return ""
}

func (km *KafkaMonitor) Gather(acc telegraf.Accumulator) error {
	if !km.initialized {
		err := km.init()
		if err != nil {
			return err
		}
		km.initialized = true
	}
	now := time.Now().Unix()
	if km.lastFetchMetadata <= 0 || now-km.lastFetchMetadata > int64(3*time.Minute) {
		km.kafkaCluster.FetchOffsets(true)
		km.lastFetchMetadata = now
	} else {
		km.kafkaCluster.FetchOffsets(false)
	}
	var consumerToDelete []string

	currentConsumerstatus := make(map[string]*consumerStatus)
	km.consumerLock.RLock()
	km.topicOffsetsLock.RLock()
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	for group, consume := range km.consumer {
		// Lazily purge consumers that haven't committed in longer than the defined interval.
		if ((now - km.ExpireGroup) * 1000) > consume.lastCommit {
			consumerToDelete = append(consumerToDelete, group)
			continue
		}
		for topic, cPartitions := range consume.topics {
			if bPartitions, ok := km.topicOffsets[topic]; ok {
				var totalLag, logSize, consumerOffset int64
				for partition, offset := range cPartitions {
					if partition < len(bPartitions) {
						lag := bPartitions[partition] - offset.offset
						if lag > 0 {
							totalLag += lag
						}
						logSize += bPartitions[partition]
						consumerOffset += offset.offset
						// fmt.Printf("%s / %s / %d : %d - %d, %d \n", group, topic, partition, bPartitions[partition], offset.offset, lag)
					}
				}
				fields := map[string]interface{}{
					"partitions": len(bPartitions),
					"lag":        totalLag,
					"offset":     consumerOffset,
					"log_size":   logSize,
				}
				tags := map[string]string{
					"group": group,
					"topic": topic,
				}

				key := topic + "/" + group
				if status, ok := km.lastConsumerStatus[key]; ok {
					duration := float64(timestamp-status.timestamp) / float64(1000)
					produceRate := float64(logSize-status.logsize) / duration
					consumeRate := float64(consumerOffset-status.offset) / duration
					if status.partitions == len(bPartitions) && produceRate >= 0 {
						fields["produce_rate_per_sec"] = produceRate
					}
					if status.partitions == len(cPartitions) && consumeRate >= 0 {
						fields["consume_rate_per_sec"] = consumeRate
					}
				}
				currentConsumerstatus[key] = &consumerStatus{
					partitions: len(bPartitions),
					timestamp:  timestamp,
					offset:     consumerOffset,
					logsize:    logSize,
				}
				// metrics := map[string]interface{}{
				// 	"name":   "kafka_consumer",
				// 	"tags":   tags,
				// 	"fields": fields,
				// }
				// fmt.Println(jsonx.MarshalAndIntend(metrics))
				acc.AddFields("kafka_consumer", fields, tags)
			}
		}
	}
	km.topicOffsetsLock.RUnlock()
	km.consumerLock.RUnlock()
	km.lastConsumerStatus = currentConsumerstatus

	if len(consumerToDelete) > 0 {
		km.consumerLock.Lock()
		for _, group := range consumerToDelete {
			delete(km.consumer, group)
		}
		km.consumerLock.Unlock()
	}
	return nil
}

func (km *KafkaMonitor) init() error {
	// if km.TopicFilter == "" {
	// 	km.TopicFilter = fmt.Sprintf("^(console-consumer-|python-kafka-consumer-|quick-|%s).*$", km.OffsetsTopic)
	// 	log.Printf("[kafka_monitor] topic filter: %s", km.TopicFilter)
	// }
	// if km.TopicFilter != "" {
	// 	re, err := regexp.Compile(km.TopicFilter)
	// 	if err != nil {
	// 		log.Printf("[kafka_monitor] Failed to compile topic filter: %s", err)
	// 	} else {
	// 		km.topicFilter = re
	// 	}
	// }

	servers := strings.Split(km.Servers, ",")
	if len(servers) <= 0 {
		return fmt.Errorf("kafka servers is empty")
	}
	if km.client != nil {
		km.client.Close()
	}

	cfg, err := km.getConfig()
	if err != nil {
		err = fmt.Errorf("failed to getConfig: %s", err)
		return err
	}
	// Connect Kafka client
	client, err := sarama.NewClient(servers, cfg)
	if err != nil {
		err = fmt.Errorf("failed to start kafka client: %s", err)
		log.Println("[kafka_monitor] ", err)
		return err
	}
	km.client = client

	km.kafkaCluster = &KafkaCluster{
		client: client,
		notify: km,
	}

	km.kafkaConsumer = NewKafkaConsumer()
	km.kafkaConsumer.client = client
	km.kafkaConsumer.notify = km
	km.kafkaConsumer.OffsetsTopic = km.OffsetsTopic
	km.kafkaConsumer.StartLatest = km.StartLatest
	km.kafkaConsumer.BackfillEarliest = km.BackfillEarliest
	km.kafkaConsumer.ReportedConsumerGroup = km.ReportedConsumerGroup
	km.kafkaConsumer.GroupWhitelist = km.GroupWhitelist
	km.kafkaConsumer.GroupBlacklist = km.GroupBlacklist
	go km.kafkaConsumer.Start()
	return nil
}

func (km *KafkaMonitor) OnDeleteTopic(topic string) {
	if !km.acceptTopic(topic) {
		return
	}
	log.Printf("[kafka_monitor] topic %s delete", topic)

	km.consumerLock.RLock()
	for _, consumerMap := range km.consumer {
		consumerMap.lock.Lock()
		delete(consumerMap.topics, topic)
		consumerMap.lock.Unlock()
	}
	km.consumerLock.RUnlock()
	km.topicOffsetsLock.Lock()
	delete(km.topicOffsets, topic)
	km.topicOffsetsLock.Unlock()
}

func (km *KafkaMonitor) OnBrokerOffsetUpdate(topic string, partitions int, partition int32, offset int64) {
	if !km.acceptTopic(topic) {
		return
	}
	km.topicOffsetsLock.Lock()
	ps, ok := km.topicOffsets[topic]
	if ok {
		if len(ps) > partitions {
			_ps := make([]int64, partitions, partitions)
			for i := 0; i < partitions; i++ {
				_ps[i] = ps[i]
			}
			ps = _ps
		}
	}
	for i := len(ps); i < partitions; i++ {
		ps = append(ps, -1)
	}
	ps[partition] = offset
	km.topicOffsets[topic] = ps
	km.topicOffsetsLock.Unlock()
}

func (km *KafkaMonitor) OnConsumerOffsetUpdate(topic string, partition int32, group string, timestamp, offset, offsetOrder int64) {
	if !km.acceptTopic(topic) {
		return
	}
	if timestamp < ((time.Now().Unix() - km.ExpireGroup) * 1000) {
		return
	}

	// Get the broker offset for this partition, as well as the partition count
	_, partitionCount := km.getBrokerOffset(topic, partition)
	if partitionCount == 0 {
		// If the returned partitionCount is zero, there was an error that was already logged. Just stop processing
		return
	}

	// Make the consumer group if it does not yet exist
	km.consumerLock.Lock()
	consumerMap, ok := km.consumer[group]
	if !ok {
		km.consumer[group] = &consumerGroup{
			topics: make(map[string][]*consumerPartition),
		}
		consumerMap = km.consumer[group]
	}
	consumerMap.lastCommit = timestamp
	km.consumerLock.Unlock()

	// For the rest of this, we need the write lock for the consumer group
	consumerMap.lock.Lock()
	defer consumerMap.lock.Unlock()

	// Get the offset ring for this partition - it always points to the earliest offset (or where to insert a new value)
	consumerPartition := km.getConsumerPartition(consumerMap, topic, partition, partitionCount)

	if consumerPartition.order > offsetOrder {
		return
	}
	consumerPartition.offset = offset
	consumerPartition.order = offsetOrder
	consumerPartition.lastCommit = timestamp
}

func (km *KafkaMonitor) OnConsumerOwnerUpdate(topic string, partition int32, group, clientHost, clientID string) {
	if !km.acceptTopic(topic) {
		return
	}
	// log.Printf("[kafka_monitor] topic: %s, partition: %d, group: %s, owner: %s, %s", topic, partition, group, clientHost, clientID)

	// Get the partition count for this partition (we don't need the actual broker offset)
	_, partitionCount := km.getBrokerOffset(topic, partition)
	if partitionCount == 0 {
		// If the returned partitionCount is zero, there was an error that was already logged. Just stop processing
		return
	}

	// Make the consumer group if it does not yet exist
	km.consumerLock.Lock()
	consumerMap, ok := km.consumer[group]
	if !ok {
		km.consumer[group] = &consumerGroup{
			topics: make(map[string][]*consumerPartition),
		}
		consumerMap = km.consumer[group]
	}
	km.consumerLock.Unlock()

	// For the rest of this, we need the write lock for the consumer group
	consumerMap.lock.Lock()
	defer consumerMap.lock.Unlock()

	// Get the consumer partition state for this partition - we don't need it, but it will properly create the topic and partitions for us
	km.getConsumerPartition(consumerMap, topic, partition, partitionCount)
	if topic, ok := consumerMap.topics[topic]; !ok || (int32(len(topic)) <= partition) {
		return
	}

	// Write the owner for the given topic/partition
	consumerMap.topics[topic][partition].owner = clientHost
	consumerMap.topics[topic][partition].clientID = clientID
}

func (km *KafkaMonitor) OnConsumerOwnersClear(group string) {
	log.Printf("[kafka_monitor] consumer group %s delete", group)

	// Make the consumer group if it does not yet exist
	km.consumerLock.Lock()
	consumerMap, ok := km.consumer[group]
	if !ok {
		// Consumer group doesn't exist, so we can't clear owners for it
		km.consumerLock.Unlock()
		return
	}
	delete(km.consumer, group)
	defer km.consumerLock.Unlock()

	// For the rest of this, we need the write lock for the consumer group
	consumerMap.lock.Lock()
	for topic, partitions := range consumerMap.topics {
		for partitionID := range partitions {
			consumerMap.topics[topic][partitionID].owner = ""
			consumerMap.topics[topic][partitionID].clientID = ""
		}
	}
	consumerMap.lock.Unlock()
}

func (km *KafkaMonitor) Close() error {
	km.kafkaConsumer.Close()
	return km.client.Close()
}

func (km *KafkaMonitor) getBrokerOffset(topic string, partition int32) (int64, int32) {
	km.topicOffsetsLock.RLock()
	defer km.topicOffsetsLock.RUnlock()

	topicPartitions, ok := km.topicOffsets[topic]
	if !ok {
		// We don't know about this topic from the brokers yet - skip consumer offsets for now
		return 0, 0
	}
	if partition < 0 {
		// This should never happen, but if it does, log an warning with the offset information for review
		return 0, 0
	}
	if partition >= int32(len(topicPartitions)) {
		// We know about the topic, but partitions have been expanded and we haven't seen that from the broker yet
		return 0, 0
	}
	if topicPartitions[partition] < 0 {
		// We know about the topic and partition, but we haven't actually gotten the broker offset yet
		return 0, 0
	}
	return topicPartitions[partition], int32(len(topicPartitions))
}

func (km *KafkaMonitor) getConsumerPartition(consumerMap *consumerGroup, topic string, partition int32, partitionCount int32) *consumerPartition {
	// Get or create the topic for the consumer
	consumerTopicMap, ok := consumerMap.topics[topic]
	if !ok {
		consumerMap.topics[topic] = make([]*consumerPartition, 0, partitionCount)
		consumerTopicMap = consumerMap.topics[topic]
	}

	// Get the partition specified
	if int(partition) >= len(consumerTopicMap) {
		// The partition count must have increased. Append enough extra partitions to our slice
		for i := int32(len(consumerTopicMap)); i < partitionCount; i++ {
			consumerTopicMap = append(consumerTopicMap, &consumerPartition{})
		}
		consumerMap.topics[topic] = consumerTopicMap
	}
	return consumerTopicMap[partition]
}

func (km *KafkaMonitor) acceptTopic(topic string) bool {
	// if km.topicFilter != nil {
	// 	return km.topicFilter.MatchString(topic)
	// }
	if topic == km.OffsetsTopic {
		return false
	}
	return true
}

func (km *KafkaMonitor) Start(acc telegraf.Accumulator) error {
	return nil
}
func (km *KafkaMonitor) Stop() {
	km.Close()
}

// func main() {
// 	km := NewKafkaMonitor()
// 	for {
// 		err := km.Gather()
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 		time.Sleep(10 * time.Second)
// 		fmt.Println("tick ...")
// 	}
// 	km.Close()
// 	// OffsetsTopic := "__consumer_offsets"
// 	// TopicFilter := fmt.Sprintf("^(?!(abcd)).*$")
// 	// fmt.Println(TopicFilter)
// 	// re, err := regexp.Compile(TopicFilter)
// 	// if err != nil {
// 	// 	log.Printf("[kafka_monitor] Failed to compile topic filter: %s", err)
// 	// 	return
// 	// }
// 	// topicFilter := re

// 	// fmt.Println(topicFilter.MatchString("__consumer_offsets"))
// }

func init() {
	inputs.Add("kafka_monitor", func() telegraf.Input {
		return NewKafkaMonitor()
	})
}
