package dkafka

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/google/cel-go/cel"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
)

type EntityType = string

const (
	Action EntityType = "action"
	Table  EntityType = "table"
)

type GenContext struct {
	block       *pbcodec.Block
	stepName    string
	transaction *pbcodec.TransactionTrace
	actionTrace *pbcodec.ActionTrace
	correlation *Correlation
	cursor      string
	step        pbbstream.ForkStep
}

type Generator2 interface {
	Apply(genContext GenContext) ([]Generation2, error)
}

type Generation2 struct {
	CeType  string         `json:"ce_type,omitempty"`
	CeId    []byte         `json:"ce_id,omitempty"`
	Key     string         `json:"key,omitempty"`
	Value   []byte         `json:"value,omitempty"`
	Headers []kafka.Header `json:"headers,omitempty"`
}

type generation struct {
	EntityType EntityType  `json:"entityType,omitempty"`
	EntityName string      `json:"entityName,omitempty"`
	CeType     string      `json:"ce_type,omitempty"`
	CeId       []byte      `json:"ce_id,omitempty"`
	Key        string      `json:"key,omitempty"`
	Value      interface{} `json:"value,omitempty"`
}

type IndexedEntry[T any] struct {
	Index int
	Entry T
}

func NewIndexedEntrySlice[T any](slice []T) []*IndexedEntry[T] {
	indexedEntrySlice := make([]*IndexedEntry[T], len(slice))
	for i, entry := range slice {
		e := new(IndexedEntry[T])
		e.Entry = entry
		e.Index = i
		indexedEntrySlice[i] = e
	}
	return indexedEntrySlice
}

type DecodeDBOp func(in *pbcodec.DBOp, blockNum uint32) (decodedDBOps *decodedDBOp, err error)

type ExtractKey func(*pbcodec.DBOp) string

func extractFullKey(dbOp *pbcodec.DBOp) string {
	return fmt.Sprintf("%s:%s", dbOp.Scope, dbOp.PrimaryKey)
}

func extractScope(dbOp *pbcodec.DBOp) string {
	return dbOp.Scope
}

func extractPrimaryKey(dbOp *pbcodec.DBOp) string {
	return dbOp.PrimaryKey
}

func indexDbOps(gc GenContext) []*IndexedEntry[*pbcodec.DBOp] {
	return orderSliceOnBlockStep(NewIndexedEntrySlice(gc.transaction.DBOpsForAction(gc.actionTrace.ExecutionIndex)), gc.step)
}

type TableGenerator struct {
	tableNames map[string]ExtractKey
	abiCodec   ABICodec
}

type void struct{}
type StringSet = map[string]void

func (tg TableGenerator) Apply(gc GenContext) (generations []Generation2, err error) {
	gens, err := tg.doApply(gc)
	if err != nil {
		return
	}
	for _, g := range gens {
		codec, err := tg.abiCodec.GetCodec(g.EntityName, gc.block.Number)
		if err != nil {
			return nil, err
		}
		zlog.Debug("marshal table", zap.String("name", g.EntityName))
		value, err := codec.Marshal(nil, g.Value)
		if err != nil {
			zlog.Debug("fail fast on codec.Marshal()", zap.Error(err))
			return nil, err
		}
		generations = append(generations, Generation2{
			CeType:  g.CeType,
			CeId:    g.CeId,
			Key:     g.Key,
			Value:   value,
			Headers: codec.GetHeaders(),
		})
	}
	zlog.Debug("return messages after marshal operation", zap.Any("nb_messages", len(generations)))
	return generations, nil
}

func (tg TableGenerator) doApply(gc GenContext) ([]generation, error) {
	indexedDbOps := indexDbOps(gc)
	generations := []generation{}
	for _, indexedDbOp := range indexedDbOps {
		dbOp := indexedDbOp.Entry
		dbOpIndex := indexedDbOp.Index
		if dbOp.Operation == pbcodec.DBOp_OPERATION_UNKNOWN {
			continue
		}
		// extractor, found := tg.tableNames[dbOp.TableName]
		extractKey, found := tg.tableNames[dbOp.TableName]
		if !found {
			continue
		}
		decodedDBOp, err := tg.abiCodec.DecodeDBOp(dbOp, gc.block.Number)
		if err != nil {
			return nil, err
		}
		key := extractKey(dbOp)
		tableCamelCase, ceType := tableCeType(dbOp.TableName)
		ceId := hashString(fmt.Sprintf(
			"%s%s%d%d%s",
			gc.cursor,
			gc.transaction.Id,
			gc.actionTrace.ExecutionIndex,
			dbOpIndex,
			gc.stepName,
		))
		value := newTableNotification(
			notificationContextMap(gc),
			actionInfoBasicMap(gc),
			decodedDBOp.asMap(dbOpRecordName(tableCamelCase), dbOpIndex),
		)
		generation := generation{
			CeId:       ceId,
			CeType:     ceType,
			Key:        key,
			Value:      value,
			EntityType: Table,
			EntityName: dbOp.TableName,
		}
		zlog.Debug("generated table message", zap.Any("generation", generation))
		generations = append(generations, generation)
	}
	zlog.Debug("return generated table messages", zap.Int("nb_generations", len(generations)))
	return generations, nil
}

type ActionGenerator2 struct {
	keyExtractors map[string]cel.Program
	abiCodec      ABICodec
}

func (ag ActionGenerator2) Apply(gc GenContext) ([]Generation2, error) {
	gens, err := ag.doApply(gc)
	if err != nil {
		return nil, err
	}
	if len(gens) > 0 {
		g := gens[0]
		codec, err := ag.abiCodec.GetCodec(g.EntityName, gc.block.Number)
		if err != nil {
			return nil, err
		}
		value, err := codec.Marshal(nil, g.Value)
		if err != nil {
			// this marshalling issue can comes from an out of date
			// version of the ABI => refresh the ABI
			err = ag.abiCodec.Refresh(gc.block.Number)
			if err != nil {
				return nil, err
			}
			value, err = codec.Marshal(nil, g.Value)
			if err != nil {
				return nil, err
			}
		}
		return []Generation2{{
			CeType:  g.CeType,
			CeId:    g.CeId,
			Key:     g.Key,
			Value:   value,
			Headers: codec.GetHeaders(),
		}}, nil
	} else {
		return nil, nil
	}

}

func (ag ActionGenerator2) doApply(gc GenContext) ([]generation, error) {
	actionName := gc.actionTrace.Action.Name
	extractor, found := ag.keyExtractors[actionName]
	if !found {
		return nil, nil
	}
	activation, err := NewActionActivation(
		gc.stepName,
		gc.transaction,
		gc.actionTrace,
	)
	if err != nil {
		return nil, err
	}
	key, err := evalString(extractor, activation)
	if err != nil {
		return nil, err
	}
	_, ceType := actionCeType(actionName)
	ceId := hashString(fmt.Sprintf(
		"%s%s%d%s",
		gc.cursor,
		gc.transaction.Id,
		gc.actionTrace.ExecutionIndex,
		gc.stepName,
	))
	jsonData := make(map[string]interface{})
	if stringData := gc.actionTrace.Action.JsonData; stringData != "" {
		err = json.Unmarshal(json.RawMessage(stringData), &jsonData)
		if err != nil {
			return nil, err
		}
	}
	indexedDbOps := indexDbOps(gc)
	dbOpsGen := make([]map[string]interface{}, len(indexedDbOps))
	for _, dbOp := range indexedDbOps {
		dbOpsGen[dbOp.Index] = newDBOpBasic(dbOp.Entry, dbOp.Index)
	}
	value := newActionNotification(
		notificationContextMap(gc),
		newActionInfo(actionInfoBasicMap(gc), jsonData, dbOpsGen),
	)
	return []generation{{
		CeId:       ceId,
		CeType:     ceType,
		Key:        key,
		Value:      value,
		EntityType: Action,
		EntityName: actionName,
	}}, nil
}

func notificationContextMap(gc GenContext) map[string]interface{} {
	status := sanitizeStatus(gc.transaction.Receipt.Status.String())

	return newNotificationContext(
		gc.block.Id,
		gc.block.Number,
		status,
		!gc.transaction.HasBeenReverted(),
		gc.stepName,
		gc.transaction.Id,
		newOptionalCorrelation(gc.correlation),
		gc.block.MustTime().UTC(),
		gc.cursor,
	)
}

func actionInfoBasicMap(gc GenContext) map[string]interface{} {
	var globalSeq uint64
	if receipt := gc.actionTrace.Receipt; receipt != nil {
		globalSeq = receipt.GlobalSequence
	}

	var authorizations []string
	for _, auth := range gc.actionTrace.Action.Authorization {
		authorizations = append(authorizations, auth.Authorization())
	}

	return newActionInfoBasic(
		gc.actionTrace.Account(),
		gc.actionTrace.Receiver,
		gc.actionTrace.Name(),
		globalSeq,
		authorizations,
	)
}
