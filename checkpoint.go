package dkafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"go.uber.org/zap"
)

const dkafkaCheckpoint = "DKafkaCheckpoint"

var blockRef = RecordSchema{
	Type:      "record",
	Name:      "BlockRef",
	Namespace: dkafkaNamespace,
	Doc:       "BlockRef represents a reference to a block and is mainly define as the pair <BlockID, BlockNum>",
	Fields: []FieldSchema{
		{
			Name: "id",
			Type: "string",
		},
		{
			Name: "num",
			Type: "long",
		},
	},
}

func newBlockRefMap(blockRef bstream.BlockRef) map[string]interface{} {
	return map[string]interface{}{
		"id":  blockRef.ID(),
		"num": blockRef.Num(),
	}
}

var CheckpointSchema = RecordSchema{
	Type:      "record",
	Name:      dkafkaCheckpoint,
	Namespace: dkafkaNamespace,
	Doc:       "Periodically emitted checkpoint used to save the current position",
	Fields: []FieldSchema{
		{
			Name: "step",
			Type: "int",
			Doc: `Step of the current block value can be: 1(New),2(Undo),3(Redo),4(Handoff),5(Irreversible),6(Stalled)
 - 1(New): First time we're seeing this block
 - 2(Undo): We are undoing this block (it was done previously)
 - 4(Redo): We are redoing this block (it was done previously)
 - 8(Handoff): The block passed a handoff from one producer to another
 - 16(Irreversible): This block passed the LIB barrier and is in chain
 - 32(Stalled): This block passed the LIB and is definitely forked out
`,
		},
		{
			Name: "block",
			Type: blockRef,
		},
		{
			Name: "headBlock",
			Type: "BlockRef",
		},
		{
			Name: "lastIrreversibleBlock",
			Type: "BlockRef",
		},
		{
			Name: "time",
			Type: map[string]string{
				"type":        "long",
				"logicalType": "timestamp-millis",
			},
		},
	},
}

type dkafkaMetaSupplier struct {
}

func (dms dkafkaMetaSupplier) GetVersion() string {
	return "1.0.0"
}
func (dms dkafkaMetaSupplier) GetSource() string {
	return "dkafka-cli"
}
func (dms dkafkaMetaSupplier) GetDomain() string {
	return "dkafka"
}
func (dms dkafkaMetaSupplier) GetCompatibility() string {
	return "FORWARD"
}
func (dms dkafkaMetaSupplier) GetType() string {
	return "notification"
}

var CheckpointMessageSchema = MessageSchema{
	CheckpointSchema,
	newMeta(dkafkaMetaSupplier{}),
}

func newCheckpointMap(cursor *forkable.Cursor, time time.Time) map[string]interface{} {
	return map[string]interface{}{
		"step":                  int(cursor.Step),
		"block":                 newBlockRefMap(cursor.Block),
		"headBlock":             newBlockRefMap(cursor.HeadBlock),
		"lastIrreversibleBlock": newBlockRefMap(cursor.LIB),
		"time":                  time,
	}
}

var ErrNoCursor = errors.New("no cursor exists")

type checkpointer interface {
	Load() (cursor string, err error)
}

func newKafkaCheckpointer(conf kafka.ConfigMap, cursorTopic string, cursorPartition int32, dataTopic string, consumerGroupID string) checkpointer {
	consumerConfig := cloneConfig(conf)
	id := strings.Replace(fmt.Sprintf("dk-%s-%s-%d", dataTopic, cursorTopic, cursorPartition), "_", "", -1)

	consumerConfig["group.id"] = consumerGroupID
	consumerConfig["enable.auto.commit"] = false

	return &kafkaCheckpointer{
		consumerConfig: consumerConfig,
		topic:          cursorTopic,
		partition:      cursorPartition,
		key:            []byte(id),
	}
}

type kafkaCheckpointer struct {
	key            []byte
	consumerConfig kafka.ConfigMap
	topic          string
	partition      int32
}

type cs struct {
	Cursor string `json:"cursor"`
}

func (c *kafkaCheckpointer) Load() (string, error) {
	zlog.Info("try to load cursor from cursor topic", zap.String("cursor_topic", c.topic), zap.Int32("cursor_partition", c.partition))
	consumer, err := kafka.NewConsumer(&c.consumerConfig)
	if err != nil {
		return "", fmt.Errorf("creating consumer: %w", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			zlog.Error("error closing consumer", zap.Error(err))
		}
	}()

	consumer.Subscribe(c.topic, nil)

	md, err := consumer.GetMetadata(&c.topic, false, 500)
	if err != nil {
		return "", fmt.Errorf("getting metadata: %w", err)
	}
	parts := md.Topics[c.topic].Partitions
	if len(parts)-1 < int(c.partition) {
		zlog.Info("requested topic or partition does not exist for cursor topic", zap.String("cursor_topic", c.topic), zap.Int32("cursor_partition", c.partition))
		return "", ErrNoCursor
	}

	low, high, err := consumer.QueryWatermarkOffsets(c.topic, c.partition, 500)
	if err != nil {
		return "", fmt.Errorf("getting low/high: %w", err)
	}

	for i := kafka.Offset(high) - 1; (i >= kafka.Offset(low)) && ((kafka.Offset(high) - i) > 50); i-- {
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
				err = ErrNoCursor
			}
			return cursor.Cursor, err
		default:
		}
	}
	return "", ErrNoCursor
}

func cloneConfig(in kafka.ConfigMap) kafka.ConfigMap {
	out := make(kafka.ConfigMap)
	for k, v := range in {
		out[k] = v
	}
	return out
}
