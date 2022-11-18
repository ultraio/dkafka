package dkafka

import (
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/google/cel-go/cel"
	"go.uber.org/zap"
)

type SaveBlock = func(*pbcodec.Block)
type DecodeDBOps = func(in []*pbcodec.DBOp, blockNum uint32) (decodedDBOps []*decodedDBOp, err error)

func saveBlockNoop(*pbcodec.Block) {
	// does nothing
}

// func marshalJSON(m proto.Message) ([]byte, error) {
// 	return json.Marshal(m)
// }

// func saveBlockJSON(block *pbcodec.Block) {
// 	saveBlock(block, marshalJSON, "json")
// }

func saveBlockJSONPB(block proto.Message) ([]byte, error) {
	s, err := (&jsonpb.Marshaler{}).MarshalToString(block)
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

func saveBlockProto(block *pbcodec.Block) {
	saveBlock(block, saveBlockJSONPB, "pb.json")
}

type MarshalFunc func(proto.Message) ([]byte, error)

func saveBlock(block *pbcodec.Block, marshal MarshalFunc, extension string) {
	byteArray, err := marshal(block)
	if err != nil {
		zlog.Error("Fail to marshal to JSON incoming block", zap.Uint32("id", block.Number), zap.Error(err))
	}
	// the WriteFile method returns an error if unsuccessful
	fileName := fmt.Sprintf("block-%d.%s", block.Number, extension)
	err = ioutil.WriteFile(fileName, byteArray, 0644)
	// handle this error
	if err != nil {
		zlog.Error("Fail to write file", zap.String("file", fileName), zap.Error(err))
	}
}

type adapter struct {
	topic                 string
	saveBlock             SaveBlock
	decodeDBOps           DecodeDBOps
	failOnUndecodableDBOP bool
	generator             Generator
	// TODO merge all headers
	headers []kafka.Header
}

func newActionsAdapter(
	topic string,
	saveBlock SaveBlock,
	decodeDBOps DecodeDBOps,
	failOnUndecodableDBOP bool,
	actionsConfJson string,
	headers []kafka.Header,
) (*adapter, error) {
	actionsConf := make(ActionsConf)
	err := json.Unmarshal(json.RawMessage(actionsConfJson), &actionsConf)
	if err != nil {
		return nil, err
	}
	generator, err := NewActionsGenerator(actionsConf)
	if err != nil {
		return nil, err
	}
	return &adapter{
		topic,
		saveBlock,
		decodeDBOps,
		failOnUndecodableDBOP,
		generator,
		headers,
	}, nil
}

func newAdapter(
	topic string,
	saveBlock SaveBlock,
	decodeDBOps DecodeDBOps,
	failOnUndecodableDBOP bool,
	eventTypeProg cel.Program,
	eventKeyProg cel.Program,
	headers []kafka.Header,
) *adapter {
	return &adapter{topic, saveBlock, decodeDBOps, failOnUndecodableDBOP, NewExpressionsGenerator(eventKeyProg, eventTypeProg), headers}
}

func (m *adapter) Adapt(blkStep BlockStep) ([]*kafka.Message, error) {
	blk := blkStep.blk
	m.saveBlock(blk)
	step := sanitizeStep(blkStep.step.String())

	if blk.Number%100 == 0 {
		zlog.Info("incoming block 1/100", zap.Uint32("blk_number", blk.Number), zap.String("step", step), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))
	}
	if blk.Number%10 == 0 {
		zlog.Debug("incoming block 1/10", zap.Uint32("blk_number", blk.Number), zap.String("step", step), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))
	}
	msgs := make([]*kafka.Message, 0, 1)
	blkTime := blk.MustTime().UTC()
	// blkTimeStr := blkTime.Format("2006-01-02T15:04:05.9Z")
	blkTimeStr := blkTime.Format(time.RFC3339)
	blkTimeBytes := []byte(blkTimeStr)

	trxs := blk.TransactionTraces()

	for _, trx := range trxs {
		transactionTracesReceived.Inc()
		status := sanitizeStatus(trx.Receipt.Status.String())
		// manage correlation
		correlation, err := getCorrelation(trx.ActionTraces)
		if err != nil {
			return nil, err
		}
		for _, act := range trx.ActionTraces {
			if !act.FilteringMatched {
				continue
			}
			actionTracesReceived.Inc()
			var jsonData json.RawMessage
			if act.Action.JsonData != "" {
				jsonData = json.RawMessage(act.Action.JsonData)
			}

			var authorizations []string
			for _, auth := range act.Action.Authorization {
				authorizations = append(authorizations, auth.Authorization())
			}

			var globalSeq uint64
			if act.Receipt != nil {
				globalSeq = act.Receipt.GlobalSequence
			}

			decodedDBOps, err := m.decodeDBOps(trx.DBOpsForAction(act.ExecutionIndex), blk.Number)
			if err != nil {
				if m.failOnUndecodableDBOP {
					return nil, err
				}
				zlog.Warn("cannot decode dbops", zap.Uint32("block_number", blk.Number), zap.Error(err))
			}

			// generation
			generations, err := m.generator.Apply(step, trx,
				act,
				decodedDBOps)

			if err != nil {
				return nil, err
			}
			var source string
			for _, entry := range m.headers {
				if entry.Key == "ce_source" {
					source = string(entry.Value)
				}
			}
			if source == "" {
				return nil, fmt.Errorf("ce_source is missing")
			}
			for _, generation := range generations {
				eosioAction := event{
					BlockNum:      blk.Number,
					BlockID:       blk.Id,
					Status:        status,
					Executed:      !trx.HasBeenReverted(),
					Step:          step,
					Correlation:   correlation,
					TransactionID: trx.Id,
					ActionInfo: ActionInfoDetails{
						Account:        act.Account(),
						Receiver:       act.Receiver,
						Action:         act.Name(),
						JSONData:       &jsonData,
						DBOps:          generation.DecodedDBOps,
						Authorization:  authorizations,
						GlobalSequence: globalSeq,
					},
				}

				headers := append(m.headers,
					kafka.Header{
						Key:   "ce_id",
						Value: hashString(fmt.Sprintf("%s%s%d%s%s%s", blk.Id, trx.Id, act.ExecutionIndex, generation.CeType, step, generation.Key)),
					},
					kafka.Header{
						Key:   "ce_type",
						Value: []byte(generation.CeType),
					},
					kafka.Header{
						Key:   "ce_time",
						Value: []byte(blkTimeBytes),
					},
					kafka.Header{
						Key:   "ce_blkstep",
						Value: []byte(step),
					},
				)
				msg := &kafka.Message{
					Key:     []byte(generation.Key),
					Headers: headers,
					Value:   eosioAction.JSON(),
					TopicPartition: kafka.TopicPartition{
						Topic:     &m.topic,
						Partition: kafka.PartitionAny,
					},
					Timestamp:     blkTime,
					TimestampType: kafka.TimestampCreateTime,
				}
				msgs = append(msgs, msg)
			}
		}
	}
	return msgs, nil
}

func hashString(data string) []byte {
	h := sha512.New()
	h.Write([]byte(data))
	return []byte(base64.StdEncoding.EncodeToString(h.Sum(nil)))
}

func sanitizeStep(step string) string {
	return strings.Title(strings.TrimPrefix(step, "STEP_"))
}
func sanitizeStatus(status string) string {
	return strings.Title(strings.TrimPrefix(status, "TRANSACTIONSTATUS_"))
}

func getCorrelation(actions []*pbcodec.ActionTrace) (correlation *Correlation, err error) {
	for _, act := range actions {
		if act.Account() == "ultra.tools" && act.Name() == "correlate" {
			jsonString := act.Action.GetJsonData()
			var out map[string]interface{}
			err = json.Unmarshal([]byte(jsonString), &out)
			if err != nil {
				err = fmt.Errorf("decoding correlate action %q: %w", jsonString, err)
				return
			}
			correlation = &Correlation{fmt.Sprint(out["payer"]), fmt.Sprint(out["correlation_id"])}
			return
		}
	}
	return
}
