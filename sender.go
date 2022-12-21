package dkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/streamingfast/bstream/forkable"
	"go.uber.org/zap"
)

const CursorHeaderKey = "dkafka_cursor"
const PreviousCursorHeaderKey = "dkafka_prev_cursor"

type location interface {
	opaqueCursor() string
	time() time.Time
	timeHeader() kafka.Header
	previousOpaqueCursor() string
}

type Sender interface {
	Send(ctx context.Context, messages []*kafka.Message, location location) error
	SaveCP(ctx context.Context, location location) error
}

type DryRunSender struct{}

func (s *DryRunSender) Send(ctx context.Context, messages []*kafka.Message, location location) error {
	for i, msg := range messages {
		outJson, err := messageToJSON(msg)
		if err != nil {
			return err
		}
		fmt.Printf("%d: %s", i, string(outJson))
	}
	return nil
}

func (s *DryRunSender) SaveCP(ctx context.Context, location location) error {
	return nil
}

type FastKafkaSender struct {
	producer *kafka.Producer
	headers  []kafka.Header
	topic    string
	abiCodec ABICodec
}

func (s *FastKafkaSender) Send(ctx context.Context, messages []*kafka.Message, location location) error {
	zlog.Debug("send messages", zap.Int("nb", len(messages)))
	for _, msg := range messages {
		msg.Headers = appendLocation(msg.Headers, location)
		if err := s.producer.Produce(msg, nil); err != nil {
			return fmt.Errorf("sender fail to Produce message to topic: '%s', with error: %w", s.topic, err)
		}
	}
	zlog.Debug("messages sent", zap.Int("nb", len(messages)))
	return nil
}

func (s *FastKafkaSender) SaveCP(ctx context.Context, location location) error {
	cursor := location.opaqueCursor()
	c, err := forkable.CursorFromOpaque(cursor)
	if err != nil {
		zlog.Error("FastKafkaSender.SaveCP() cannot decode cursor", zap.String("cursor", cursor), zap.Error(err))
		return err
	}
	zlog.Info("save checkpoint",
		zap.String("cursor", cursor),
		zap.Stringer("plain_cursor", c),
		zap.Stringer("cursor_block", c.Block),
		zap.Stringer("cursor_head_block", c.HeadBlock),
		zap.Stringer("cursor_LIB", c.LIB),
	)
	checkpoint := newCheckpointMap(c, location.time())
	codec, err := s.abiCodec.GetCodec(dkafkaCheckpoint, 0)
	if err != nil {
		return fmt.Errorf("SaveCP() fail to get codec for %s: %w", dkafkaCheckpoint, err)
	}
	value, err := codec.Marshal(nil, checkpoint)
	if err != nil {
		return fmt.Errorf("SaveCP() fail to marshal %s: %w", dkafkaCheckpoint, err)
	}
	ce_id := hashString(cursor)
	headers := append(s.headers,
		// add codec specific content type
		codec.GetHeaders()...,
	)
	headers = append(headers,
		kafka.Header{
			Key:   "ce_id",
			Value: ce_id,
		},
		kafka.Header{
			Key:   "ce_type",
			Value: []byte(dkafkaCheckpoint),
		},
		location.timeHeader(),
		newCursorHeader(cursor),
		newPreviousCursorHeader(location.previousOpaqueCursor()),
	)

	msg := kafka.Message{
		Key:     nil,
		Headers: headers,
		Value:   value,
		TopicPartition: kafka.TopicPartition{
			Topic:     &s.topic,
			Partition: kafka.PartitionAny,
		},
	}
	if err := s.producer.Produce(&msg, nil); err != nil {
		return err
	}
	return nil
}

func appendLocation(headers []kafka.Header, location location) []kafka.Header {
	return append(headers, newCursorHeader(location.opaqueCursor()),
		newPreviousCursorHeader(location.previousOpaqueCursor()))
}

func newCursorHeader(cursor string) kafka.Header {
	return kafka.Header{
		Key:   CursorHeaderKey,
		Value: []byte(cursor),
	}
}

func newPreviousCursorHeader(cursor string) kafka.Header {
	return kafka.Header{
		Key:   PreviousCursorHeaderKey,
		Value: []byte(cursor),
	}
}

func NewFastSender(ctx context.Context, producer *kafka.Producer, topic string, headers []kafka.Header, abiCodec ABICodec) Sender {
	ks := FastKafkaSender{
		producer: producer,
		headers:  headers,
		topic:    topic,
		abiCodec: abiCodec,
	}
	return &ks
}

func getKafkaProducer(conf kafka.ConfigMap) (*kafka.Producer, error) {
	producerConfig := cloneConfig(conf)
	return kafka.NewProducer(&producerConfig)
}

type fakeMessage struct {
	Topic     string          `json:"topic"`
	Headers   []string        `json:"headers"`
	Partition int             `json:"partition"`
	Offset    int             `json:"offset"`
	TS        uint64          `json:"ts"`
	Key       string          `json:"key"`
	Payload   json.RawMessage `json:"payload"`
}

func messageToJSON(msg *kafka.Message) (json.RawMessage, error) {
	out := &fakeMessage{
		Payload: json.RawMessage(msg.Value),
		Key:     string(msg.Key),
	}
	for _, h := range msg.Headers {
		out.Headers = append(out.Headers, h.Key, string(h.Value))
	}
	return json.Marshal(out)
}
