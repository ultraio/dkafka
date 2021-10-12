package dkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

var NoCursorErr = errors.New("no cursor exists")

type checkpointer interface {
	Save(cursor string) error
	Load() (cursor string, err error)
}

type nilCheckpointer struct{}

func (n *nilCheckpointer) Save(string) error {
	return nil
}

func (n *nilCheckpointer) Load() (string, error) {
	return "", NoCursorErr
}

func newKafkaCheckpointer(conf kafka.ConfigMap, cursorTopic string, cursorPartition int32, dataTopic string, consumerGroupID string, producer *kafka.Producer) *kafkaCheckpointer {
	consumerConfig := cloneConfig(conf)
	id := strings.Replace(fmt.Sprintf("dk-%s-%s-%d", dataTopic, cursorTopic, cursorPartition), "_", "", -1)

	consumerConfig["group.id"] = consumerGroupID
	consumerConfig["enable.auto.commit"] = false

	return &kafkaCheckpointer{
		consumerConfig: consumerConfig,
		topic:          cursorTopic,
		partition:      cursorPartition,
		key:            []byte(id),
		producer:       producer,
	}
}

type kafkaCheckpointer struct {
	key            []byte
	producer       *kafka.Producer
	consumerConfig kafka.ConfigMap
	topic          string
	partition      int32
}

// in case we need it
//func newFileCheckpointer(filename string) *localFileCheckpointer {
//	return &localFileCheckpointer{
//		filename: filename,
//	}
//}
//
//type localFileCheckpointer struct {
//	filename string
//}
//
//func (c *localFileCheckpointer) Save(cursor string) error {
//	dat := []byte(cursor)
//	return ioutil.WriteFile(c.filename, dat, 0644)
//}
//
//func (c *localFileCheckpointer) Load() (string, error) {
//	dat, err := ioutil.ReadFile(c.filename)
//	if os.IsNotExist(err) {
//		return "", NoCursorErr
//	}
//	return string(dat), err
//}

type cs struct {
	Cursor string `json:"cursor"`
}

func (c *kafkaCheckpointer) Save(cursor string) error {
	v, err := json.Marshal(cs{Cursor: cursor})
	if err != nil {
		return err
	}
	msg := &kafka.Message{
		Key: c.key,
		TopicPartition: kafka.TopicPartition{
			Topic:     &c.topic,
			Partition: c.partition,
		},
		Value: v,
	}
	return c.producer.Produce(msg, nil)
}

func (c *kafkaCheckpointer) Load() (string, error) {
	consumer, err := kafka.NewConsumer(&c.consumerConfig)
	if err != nil {
		return "", fmt.Errorf("creating consumer: %w", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("error closing consumer: %s", err)
		}
	}()

	consumer.Subscribe(c.topic, nil)

	md, err := consumer.GetMetadata(&c.topic, false, 500)
	if err != nil {
		return "", fmt.Errorf("getting metadata: %w", err)
	}
	parts := md.Topics[c.topic].Partitions
	if len(parts) == 0 {
		zlog.Info("cursor topic does not exist, creating", zap.String("cursor_topic", c.topic))
		err := createKafkaCursorTopic(consumer, c.topic, len(md.Brokers))
		if err != nil {
			return "", err
		}
	} else if len(parts)-1 < int(c.partition) {
		return "", fmt.Errorf("requested cursor partition does not exist in cursor topic")
	}

	low, high, err := consumer.QueryWatermarkOffsets(c.topic, c.partition, 500)
	if err != nil {
		return "", fmt.Errorf("getting low/high: %w", err)
	}

	for i := kafka.Offset(high) - 1; i >= kafka.Offset(low); i-- {
		err = consumer.Assign([]kafka.TopicPartition{
			{
				Topic:     &c.topic,
				Partition: c.partition,
				Offset:    i,
			}})

		if err != nil {
			return "", err
		}

		ev := consumer.Poll(1000)
		switch event := ev.(type) {
		case kafka.Error:
			return "", event
		case *kafka.Message:
			cursor := cs{}
			if err := json.Unmarshal(event.Value, &cursor); err != nil {
				return "", err
			}
			if strings.HasPrefix(string(event.Key), "dk-") {
				if string(event.Key) != string(c.key) {
					return "", fmt.Errorf("invalid key for cursor: expected %s, got %s -- are you reading from the right partition?", string(c.key), string(event.Key))
				}
			}
			if cursor.Cursor == "" {
				err = NoCursorErr
			}
			return cursor.Cursor, err
		default:
		}
	}
	return "", NoCursorErr
}

func cloneConfig(in kafka.ConfigMap) kafka.ConfigMap {
	out := make(kafka.ConfigMap)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func createKafkaCursorTopic(c *kafka.Consumer, cursorTopic string, maxAvailableBrokers int) error {
	adminCli, err := kafka.NewAdminClientFromConsumer(c)
	if err != nil {
		return fmt.Errorf("creating admin client: %w", err)
	}
	numParts := 10
	replicationFactor := 3
	if replicationFactor > maxAvailableBrokers {
		replicationFactor = maxAvailableBrokers
	}

	results, err := adminCli.CreateTopics(
		context.Background(),
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             cursorTopic,
			NumPartitions:     numParts,
			ReplicationFactor: replicationFactor}},
		// Admin options
		kafka.SetAdminOperationTimeout(time.Second*10))
	if err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}

	zlog.Info("creating topic", zap.Any("results", results), zap.Int("num_partitions", numParts), zap.Int("replication_factor", replicationFactor))
	return nil
}
