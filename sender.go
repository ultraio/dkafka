package dkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

type sender interface {
	Send(msg *kafka.Message) error
	CommitIfAfter(ctx context.Context, cursor string, minimumDelay time.Duration) error
	Commit(ctx context.Context, cursor string) error
}

type kafkaSender struct {
	sync.RWMutex
	lastCommit      time.Time
	trxStarted      bool
	producer        *kafka.Producer
	cp              checkpointer
	useTransactions bool
}

func (s *kafkaSender) Send(msg *kafka.Message) error {
	s.RLock()
	defer s.RUnlock()
	return s.producer.Produce(msg, nil)
}

func (s *kafkaSender) Close(ctx context.Context) {
	if s.useTransactions {
		if err := s.producer.CommitTransaction(ctx); err != nil {
			zlog.Error("cannot commit transaction on close", zap.Error(err))
		}
	}
	s.producer.Close()
}

func (s *kafkaSender) CommitIfAfter(ctx context.Context, cursor string, minimumDelay time.Duration) error {
	if time.Since(s.lastCommit) > minimumDelay {
		zlog.Debug("commiting cursor")
		return s.Commit(ctx, cursor)
	}
	return nil
}

func (s *kafkaSender) Commit(ctx context.Context, cursor string) error {
	s.Lock() // full write lock
	defer s.Unlock()

	if err := s.cp.Save(cursor); err != nil {
		return fmt.Errorf("saving cursor: %w", err)
	}
	s.lastCommit = time.Now()

	if s.useTransactions {
		if err := s.producer.CommitTransaction(ctx); err != nil {
			return fmt.Errorf("committing transaction: %w", err)
		}

		if err := s.producer.BeginTransaction(); err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}
	}
	return nil
}

func getKafkaProducer(conf kafka.ConfigMap, name string) (*kafka.Producer, error) {
	producerConfig := cloneConfig(conf)
	if name != "" {
		producerConfig["transactional.id"] = name
	}
	return kafka.NewProducer(&producerConfig)
}

func getKafkaSender(producer *kafka.Producer, cp checkpointer, useTransactions bool) (*kafkaSender, error) {
	if useTransactions {
		ctx := context.Background() //FIXME
		if err := producer.InitTransactions(ctx); err != nil {
			return nil, fmt.Errorf("running InitTransactions: %w", err)
		}

		// initial transaction
		if err := producer.BeginTransaction(); err != nil {
			return nil, fmt.Errorf("running BeginTransaction: %w", err)
		}
	}

	return &kafkaSender{
		cp:              cp,
		producer:        producer,
		useTransactions: useTransactions,
	}, nil
}

type dryRunSender struct{}

type fakeMessage struct {
	Topic     string          `json:"topic"`
	Headers   []string        `json:"headers"`
	Partition int             `json:"partition"`
	Offset    int             `json:"offset"`
	TS        uint64          `json:"ts"`
	Key       string          `json:"key"`
	Payload   json.RawMessage `json:"payload"`
}

func (s *dryRunSender) Send(msg *kafka.Message) error {
	out := &fakeMessage{
		Payload: json.RawMessage(msg.Value),
		Key:     string(msg.Key),
	}
	for _, h := range msg.Headers {
		out.Headers = append(out.Headers, h.Key, string(h.Value))
	}
	outjson, err := json.Marshal(out)
	if err != nil {
		return err
	}
	fmt.Println(string(outjson))
	return nil
}

func (s *dryRunSender) CommitIfAfter(context.Context, string, time.Duration) error {
	return nil
}

func (s *dryRunSender) Commit(context.Context, string) error {
	return nil
}
