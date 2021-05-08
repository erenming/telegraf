package kafka_monitor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"sync"

	"github.com/Shopify/sarama"
)

type ConsumerNotify interface {
	OnConsumerOffsetUpdate(topic string, partition int32, group string, timestamp, offset, offsetOrder int64)
	OnConsumerOwnerUpdate(topic string, partition int32, group string, clientHost, clientID string)
	OnConsumerOwnersClear(group string)
}

type KafkaConsumer struct {
	OffsetsTopic          string
	StartLatest           bool
	BackfillEarliest      bool
	ReportedConsumerGroup string
	GroupWhitelist        string
	GroupBlacklist        string

	notify ConsumerNotify
	client sarama.Client

	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	running        sync.WaitGroup
	quitChannel    chan struct{}
}

func NewKafkaConsumer() *KafkaConsumer {
	return &KafkaConsumer{
		OffsetsTopic:          "__consumer_offsets",
		StartLatest:           false,
		BackfillEarliest:      true,
		quitChannel:           make(chan struct{}),
		ReportedConsumerGroup: "",
	}
}

func (kc *KafkaConsumer) Configure() error {
	if kc.GroupWhitelist != "" {
		re, err := regexp.Compile(kc.GroupWhitelist)
		if err != nil {
			return fmt.Errorf("Failed to compile group whitelist: %s", err)
		}
		kc.groupWhitelist = re
	}

	if kc.GroupBlacklist != "" {
		re, err := regexp.Compile(kc.GroupBlacklist)
		if err != nil {
			return fmt.Errorf("Failed to compile group blacklist: %s", err)
		}
		kc.groupBlacklist = re
	}
	return nil
}

func (kc *KafkaConsumer) Start() error {
	// Create the consumer from the client
	consumer, err := sarama.NewConsumerFromClient(kc.client)
	if err != nil {
		err = fmt.Errorf("failed to get new consumer: %s", err)
		log.Printf("[kafka_consumer] %s", err)
		// client.Close()
		return err
	}

	// Get a partition count for the consumption topic
	partitions, err := kc.client.Partitions(kc.OffsetsTopic)
	if err != nil {
		err = fmt.Errorf("failed to get partition count from %s: %s", kc.OffsetsTopic, err)
		log.Printf("[kafka_consumer] %s", err)
		// client.Close()
		return err
	}

	// Default to bootstrapping the offsets topic, unless configured otherwise
	startFrom := sarama.OffsetOldest
	if kc.StartLatest {
		startFrom = sarama.OffsetNewest
	}

	log.Printf("[kafka_consumer] starting consumers topic: %s, partitions: %d", kc.OffsetsTopic, len(partitions))
	for _, partition := range partitions {
		pconsumer, err := consumer.ConsumePartition(kc.OffsetsTopic, partition, startFrom)
		if err != nil {
			err = fmt.Errorf("failed to consume %s partition %d: %s", kc.OffsetsTopic, partition, err)
			log.Printf("[kafka_consumer] %s", err)
			return err
		}
		kc.running.Add(1)
		go kc.partitionConsumer(pconsumer, nil)
	}

	if kc.StartLatest && kc.BackfillEarliest {
		log.Println("[kafka_consumer] backfilling consumer offsets")
		// Note: since we are consuming each partition twice,
		// we need a second consumer instance
		consumer, err := sarama.NewConsumerFromClient(kc.client)
		if err != nil {
			log.Println("[kafka_consumer] failed to get new consumer: ", err)
			return err
		}

		waiting := len(partitions)
		backfillStartedChan := make(chan error)
		for _, partition := range partitions {
			go func(partition int32) {
				backfillStartedChan <- kc.startBackfillPartitionConsumer(partition, kc.client, consumer)
			}(partition)
		}
		for waiting > 0 {
			select {
			case err := <-backfillStartedChan:
				waiting--
				if err != nil {
					return err
				}
			case <-kc.quitChannel:
				return nil
			}
		}
	}
	return nil
}

func (kc *KafkaConsumer) Close() error {
	close(kc.quitChannel)
	kc.running.Wait()
	return nil
}

func (kc *KafkaConsumer) partitionConsumer(consumer sarama.PartitionConsumer, stopAtOffset *int64) {
	defer kc.running.Done()
	defer consumer.AsyncClose()
	for {
		select {
		case msg := <-consumer.Messages():
			if stopAtOffset != nil && msg.Offset >= *stopAtOffset {
				// module.Log.Debug("backfill consumer reached target offset, terminating",
				// 	zap.Int32("partition", msg.Partition),
				// 	zap.Int64("offset", stopAtOffset.Value),
				// )
				// log.Println()
				return
			}
			if kc.ReportedConsumerGroup != "" {
				// emulating a consumer which should commit (lastSeenOffset+1)
				kc.notify.OnConsumerOffsetUpdate(kc.OffsetsTopic, msg.Partition, kc.ReportedConsumerGroup, time.Now().Unix()*1000, msg.Offset+1, msg.Timestamp.Unix())
			}
			kc.processConsumerOffsetsMessage(msg)
		case err := <-consumer.Errors():
			log.Printf("[kafka_consumer] consume error, topic: %s, partition: %d, %s", err.Topic, err.Partition, err.Err.Error())
		case <-kc.quitChannel:
			return
		}
	}
}

func (kc *KafkaConsumer) processConsumerOffsetsMessage(msg *sarama.ConsumerMessage) {
	logInfo := fmt.Sprintf("offset_topic: %s, offset_partition: %d, offset_offset: %d", msg.Topic, msg.Partition, msg.Offset)
	// log.Printf("[kafka_consumer] %s", logInfo)

	if len(msg.Value) == 0 {
		// Tombstone message - we don't handle them for now
		// log.Printf("[kafka_consumer] %s, dropped tombstone", logInfo)
		return
	}

	var keyver int16
	keyBuffer := bytes.NewBuffer(msg.Key)
	err := binary.Read(keyBuffer, binary.BigEndian, &keyver)
	if err != nil {
		log.Printf("[kafka_consumer] %s, failed to decode, no key version", logInfo)
		return
	}
	switch keyver {
	case 0, 1:
		kc.decodeKeyAndOffset(msg.Offset, keyBuffer, msg.Value, logInfo)
	case 2:
		kc.decodeGroupMetadata(keyBuffer, msg.Value, logInfo)
	default:
		log.Printf("[kafka_consumer] failed to decode, key version %d", keyver)
	}
}

func (kc *KafkaConsumer) startBackfillPartitionConsumer(partition int32, client sarama.Client, consumer sarama.Consumer) error {
	pconsumer, err := consumer.ConsumePartition(kc.OffsetsTopic, partition, sarama.OffsetOldest)
	if err != nil {
		err = fmt.Errorf("failed to consume partition, topic: %s, partition: %d, %s", kc.OffsetsTopic, partition, err)
		log.Printf("[kafka_consumer] %s", err)
		return err
	}

	// We check for an empty partition after building the consumer, otherwise we
	// could be unlucky enough to observe a nonempty partition
	// whose only segment expires right after we check.
	oldestOffset, err := client.GetOffset(kc.OffsetsTopic, partition, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("failed to get oldest offset, topic: %s, partition: %d, %s", kc.OffsetsTopic, partition, err)
	}

	newestOffset, err := client.GetOffset(kc.OffsetsTopic, partition, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("failed to get newest offset, topic: %s, partition: %d, %s", kc.OffsetsTopic, partition, err)
	}
	if newestOffset > 0 {
		// GetOffset returns the next (not yet published) offset, but we want the latest published offset.
		newestOffset--
	}

	if oldestOffset >= newestOffset {
		// log.Printf("[kafka_consumer] not backfilling empty partition, topic: %s, partition: %d, oldestOffset: %d, newestOffset: %d",
		// 	kc.OffsetsTopic, partition, oldestOffset, newestOffset)
		pconsumer.AsyncClose()
	} else {
		kc.running.Add(1)
		go kc.partitionConsumer(pconsumer, &newestOffset)
	}
	return nil
}

type offsetKey struct {
	Group     string
	Topic     string
	Partition int32
	ErrorAt   string
}
type offsetValue struct {
	Offset    int64
	Timestamp int64
	ErrorAt   string
}
type metadataHeader struct {
	ProtocolType          string
	Generation            int32
	Protocol              string
	Leader                string
	CurrentStateTimestamp int64
}
type metadataMember struct {
	MemberID         string
	GroupInstanceID  string
	ClientID         string
	ClientHost       string
	RebalanceTimeout int32
	SessionTimeout   int32
	Assignment       map[string][]int32
}

func (kc *KafkaConsumer) acceptConsumerGroup(group string) bool {
	if (kc.groupWhitelist != nil) && (!kc.groupWhitelist.MatchString(group)) {
		return false
	}
	if (kc.groupBlacklist != nil) && kc.groupBlacklist.MatchString(group) {
		return false
	}
	return true
}

func (kc *KafkaConsumer) decodeKeyAndOffset(offsetOrder int64, keyBuffer *bytes.Buffer, value []byte, logInfo string) {
	// Version 0 and 1 keys are decoded the same way
	offsetKey, errorAt := decodeOffsetKeyV0(keyBuffer)
	logInfo = fmt.Sprintf("%s, offset message, topic: %s, partition: %d, group: %s", logInfo, offsetKey.Topic, offsetKey.Partition, offsetKey.Group)
	if errorAt != "" {
		log.Printf("[kafka_consumer] failed to decode, %s, %s", logInfo, errorAt)
		return
	}

	if !kc.acceptConsumerGroup(offsetKey.Group) {
		// log.Printf("[kafka_consumer] %s, dropped by whitelist", logInfo)
		return
	}

	var valueVersion int16
	valueBuffer := bytes.NewBuffer(value)
	err := binary.Read(valueBuffer, binary.BigEndian, &valueVersion)
	if err != nil {
		log.Printf("[kafka_consumer] failed to decode, %s, no value version", logInfo)
		return
	}

	switch valueVersion {
	case 0, 1:
		kc.decodeAndSendOffset(offsetOrder, offsetKey, valueBuffer, logInfo, decodeOffsetValueV0)
	case 3:
		kc.decodeAndSendOffset(offsetOrder, offsetKey, valueBuffer, logInfo, decodeOffsetValueV3)
	default:
		log.Printf("[kafka_consumer] failed to decode, %s, invalid version %d", logInfo, valueVersion)
	}
}

func (kc *KafkaConsumer) decodeAndSendOffset(offsetOrder int64, offsetKey offsetKey, valueBuffer *bytes.Buffer, logInfo string, decoder func(*bytes.Buffer) (offsetValue, string)) {
	offsetValue, errorAt := decoder(valueBuffer)
	if errorAt != "" {
		log.Printf("[kafka_consumer] failed to decode, %s, offset: %d, timestamp: %d, %s", logInfo, offsetValue.Offset, offsetValue.Timestamp, errorAt)
		return
	}
	// log.Printf("[kafka_consumer] %s, offset: %d, timestamp: %d", logInfo, offsetValue.Offset, offsetValue.Timestamp)
	kc.notify.OnConsumerOffsetUpdate(offsetKey.Topic, int32(offsetKey.Partition), offsetKey.Group, offsetValue.Timestamp, offsetValue.Offset, offsetOrder)
}

func (kc *KafkaConsumer) decodeGroupMetadata(keyBuffer *bytes.Buffer, value []byte, logInfo string) {
	group, err := readString(keyBuffer)
	if err != nil {
		log.Printf("[kafka_consumer] failed to decode, metadata message, %s, fail to read group, %s", logInfo, err)
		return
	}

	if !kc.acceptConsumerGroup(group) {
		// log.Printf("[kafka_consumer] %s, dropped by whitelist", logInfo)
		return
	}

	var valueVersion int16
	valueBuffer := bytes.NewBuffer(value)
	err = binary.Read(valueBuffer, binary.BigEndian, &valueVersion)
	if err != nil {
		log.Printf("[kafka_consumer] failed to decode, metadata message, %s, group: %s, no value version", logInfo, group)
		return
	}

	switch valueVersion {
	case 0, 1, 2, 3:
		logInfo = fmt.Sprintf("metadata message, %s, group: %s", logInfo, group)
		kc.decodeAndSendGroupMetadata(valueVersion, group, valueBuffer, logInfo)
	default:
		log.Printf("[kafka_consumer] failed to decode, metadata message, %s, group: %s, invalid version %d", logInfo, group, valueVersion)
	}
}

func (kc *KafkaConsumer) decodeAndSendGroupMetadata(valueVersion int16, group string, valueBuffer *bytes.Buffer, logInfo string) {
	var metadataHeader metadataHeader
	var errorAt string
	switch valueVersion {
	case 2, 3:
		metadataHeader, errorAt = decodeMetadataValueHeaderV2(valueBuffer)
	default:
		metadataHeader, errorAt = decodeMetadataValueHeader(valueBuffer)
	}
	logInfo = fmt.Sprintf("%s, protocol_type: %s, generation: %d, protocol: %s, leader: %s, current_state_timestamp: %d",
		logInfo,
		metadataHeader.ProtocolType,
		metadataHeader.Generation,
		metadataHeader.Protocol,
		metadataHeader.Leader,
		metadataHeader.CurrentStateTimestamp)
	if errorAt != "" {
		log.Printf("[kafka_consumer] failed to decode, %s, %s", logInfo, errorAt)
		return
	}
	// log.Printf("[kafka_consumer] %s, group metadata", logInfo)
	if metadataHeader.ProtocolType != "consumer" {
		log.Printf("[kafka_consumer] %s, skipped metadata because of unknown protocolType", logInfo)
		return
	}

	var memberCount int32
	err := binary.Read(valueBuffer, binary.BigEndian, &memberCount)
	if err != nil {
		log.Printf("[kafka_consumer] failed to decode, %s, no member size", logInfo)
		return
	}

	// If memberCount is zero, clear all ownership
	if memberCount == 0 {
		log.Printf("[kafka_consumer] %s, clear owners", logInfo)
		kc.notify.OnConsumerOwnersClear(group)
		return
	}

	count := int(memberCount)
	for i := 0; i < count; i++ {
		member, errorAt := decodeMetadataMember(valueBuffer, valueVersion)
		if errorAt != "" {
			log.Printf("[kafka_consumer] failed to decode, %s, %s", logInfo, errorAt)
			return
		}

		for topic, partitions := range member.Assignment {
			for _, partition := range partitions {
				kc.notify.OnConsumerOwnerUpdate(topic, partition, group, member.ClientHost, member.ClientID)
			}
		}
	}
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen int16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	if strlen == -1 {
		return "", nil
	}

	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}

func decodeMetadataValueHeader(buf *bytes.Buffer) (metadataHeader, string) {
	var err error
	metadataHeader := metadataHeader{}

	metadataHeader.ProtocolType, err = readString(buf)
	if err != nil {
		return metadataHeader, "protocol_type"
	}
	err = binary.Read(buf, binary.BigEndian, &metadataHeader.Generation)
	if err != nil {
		return metadataHeader, "generation"
	}
	metadataHeader.Protocol, err = readString(buf)
	if err != nil {
		return metadataHeader, "protocol"
	}
	metadataHeader.Leader, err = readString(buf)
	if err != nil {
		return metadataHeader, "leader"
	}
	return metadataHeader, ""
}

func decodeMetadataValueHeaderV2(buf *bytes.Buffer) (metadataHeader, string) {
	var err error
	metadataHeader := metadataHeader{}

	metadataHeader.ProtocolType, err = readString(buf)
	if err != nil {
		return metadataHeader, "protocol_type"
	}
	err = binary.Read(buf, binary.BigEndian, &metadataHeader.Generation)
	if err != nil {
		return metadataHeader, "generation"
	}
	metadataHeader.Protocol, err = readString(buf)
	if err != nil {
		return metadataHeader, "protocol"
	}
	metadataHeader.Leader, err = readString(buf)
	if err != nil {
		return metadataHeader, "leader"
	}
	err = binary.Read(buf, binary.BigEndian, &metadataHeader.CurrentStateTimestamp)
	if err != nil {
		return metadataHeader, "current_state_timestamp"
	}
	return metadataHeader, ""
}

func decodeGroupInstanceID(buf *bytes.Buffer, memberVersion int16) (string, error) {
	if memberVersion == 3 {
		return readString(buf)
	}
	return "", nil
}

func decodeMetadataMember(buf *bytes.Buffer, memberVersion int16) (metadataMember, string) {
	var err error
	memberMetadata := metadataMember{}

	memberMetadata.MemberID, err = readString(buf)
	if err != nil {
		return memberMetadata, "member_id"
	}
	memberMetadata.GroupInstanceID, err = decodeGroupInstanceID(buf, memberVersion)
	if err != nil {
		return memberMetadata, "group_instance_id"
	}
	memberMetadata.ClientID, err = readString(buf)
	if err != nil {
		return memberMetadata, "client_id"
	}
	memberMetadata.ClientHost, err = readString(buf)
	if err != nil {
		return memberMetadata, "client_host"
	}
	if memberVersion >= 1 {
		err = binary.Read(buf, binary.BigEndian, &memberMetadata.RebalanceTimeout)
		if err != nil {
			return memberMetadata, "rebalance_timeout"
		}
	}
	err = binary.Read(buf, binary.BigEndian, &memberMetadata.SessionTimeout)
	if err != nil {
		return memberMetadata, "session_timeout"
	}

	var subscriptionBytes int32
	err = binary.Read(buf, binary.BigEndian, &subscriptionBytes)
	if err != nil {
		return memberMetadata, "subscription_bytes"
	}
	if subscriptionBytes > 0 {
		buf.Next(int(subscriptionBytes))
	}

	var assignmentBytes int32
	err = binary.Read(buf, binary.BigEndian, &assignmentBytes)
	if err != nil {
		return memberMetadata, "assignment_bytes"
	}

	if assignmentBytes > 0 {
		assignmentData := buf.Next(int(assignmentBytes))
		assignmentBuf := bytes.NewBuffer(assignmentData)
		var consumerProtocolVersion int16
		err = binary.Read(assignmentBuf, binary.BigEndian, &consumerProtocolVersion)
		if err != nil {
			return memberMetadata, "consumer_protocol_version"
		}
		if consumerProtocolVersion < 0 {
			return memberMetadata, "consumer_protocol_version"
		}
		assignment, errorAt := decodeMemberAssignmentV0(assignmentBuf)
		if errorAt != "" {
			return memberMetadata, "assignment"
		}
		memberMetadata.Assignment = assignment
	}

	return memberMetadata, ""
}

func decodeMemberAssignmentV0(buf *bytes.Buffer) (map[string][]int32, string) {
	var err error
	var topics map[string][]int32
	var numTopics, numPartitions, partitionID, userDataLen int32

	err = binary.Read(buf, binary.BigEndian, &numTopics)
	if err != nil {
		return topics, "assignment_topic_count"
	}

	topicCount := int(numTopics)
	topics = make(map[string][]int32, numTopics)
	for i := 0; i < topicCount; i++ {
		topicName, err := readString(buf)
		if err != nil {
			return topics, "topic_name"
		}

		err = binary.Read(buf, binary.BigEndian, &numPartitions)
		if err != nil {
			return topics, "assignment_partition_count"
		}
		partitionCount := int(numPartitions)
		topics[topicName] = make([]int32, numPartitions)
		for j := 0; j < partitionCount; j++ {
			err = binary.Read(buf, binary.BigEndian, &partitionID)
			if err != nil {
				return topics, "assignment_partition_id"
			}
			topics[topicName][j] = int32(partitionID)
		}
	}

	err = binary.Read(buf, binary.BigEndian, &userDataLen)
	if err != nil {
		return topics, "user_bytes"
	}
	if userDataLen > 0 {
		buf.Next(int(userDataLen))
	}

	return topics, ""
}

func decodeOffsetKeyV0(buf *bytes.Buffer) (offsetKey, string) {
	var err error
	offsetKey := offsetKey{}

	offsetKey.Group, err = readString(buf)
	if err != nil {
		return offsetKey, "group"
	}
	offsetKey.Topic, err = readString(buf)
	if err != nil {
		return offsetKey, "topic"
	}
	err = binary.Read(buf, binary.BigEndian, &offsetKey.Partition)
	if err != nil {
		return offsetKey, "partition"
	}
	return offsetKey, ""
}

func decodeOffsetValueV0(valueBuffer *bytes.Buffer) (offsetValue, string) {
	var err error
	offsetValue := offsetValue{}

	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Offset)
	if err != nil {
		return offsetValue, "offset"
	}
	_, err = readString(valueBuffer)
	if err != nil {
		return offsetValue, "metadata"
	}
	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Timestamp)
	if err != nil {
		return offsetValue, "timestamp"
	}
	return offsetValue, ""
}

func decodeOffsetValueV3(valueBuffer *bytes.Buffer) (offsetValue, string) {
	var err error
	offsetValue := offsetValue{}

	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Offset)
	if err != nil {
		return offsetValue, "offset"
	}
	var leaderEpoch int32
	err = binary.Read(valueBuffer, binary.BigEndian, &leaderEpoch)
	if err != nil {
		return offsetValue, "leaderEpoch"
	}
	_, err = readString(valueBuffer)
	if err != nil {
		return offsetValue, "metadata"
	}
	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Timestamp)
	if err != nil {
		return offsetValue, "timestamp"
	}
	return offsetValue, ""
}
