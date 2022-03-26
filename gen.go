package dkafka

import (
	"encoding/json"
	"fmt"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/google/cel-go/cel"
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
}

type Generator2 interface {
	Apply(genContext GenContext) ([]Generation2, error)
}

type Generation2 struct {
	CeType string `json:"ce_type,omitempty"`
	CeId   []byte `json:"ce_id,omitempty"`
	Key    string `json:"key,omitempty"`
	Value  []byte `json:"value,omitempty"`
}

type generation struct {
	EntityType EntityType  `json:"entityType,omitempty"`
	EntityName string      `json:"entityName,omitempty"`
	CeType     string      `json:"ce_type,omitempty"`
	CeId       []byte      `json:"ce_id,omitempty"`
	Key        string      `json:"key,omitempty"`
	Value      interface{} `json:"value,omitempty"`
}

type DecodeDBOp = func(in *pbcodec.DBOp, blockNum uint32) (decodedDBOps *decodedDBOp, err error)

type TableGenerator struct {
	tableNames StringSet
	abiCodec   ABICodec
}

type void struct{}
type StringSet = map[string]void

var empty = void{}

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
		value, err := codec.Marshal(nil, g.Value)
		if err != nil {
			return nil, err
		}
		generations = append(generations, Generation2{
			CeType: g.CeType,
			CeId:   g.CeId,
			Key:    g.Key,
			Value:  value,
		})
	}
	zlog.Debug("return messages after marshal operation", zap.Any("nb_messages", len(generations)))
	return generations, nil
}

func (tg TableGenerator) doApply(gc GenContext) ([]generation, error) {
	dbOps := gc.transaction.DBOpsForAction(gc.actionTrace.ExecutionIndex)
	generations := []generation{}
	for _, dbOp := range dbOps {
		if dbOp.Operation == pbcodec.DBOp_OPERATION_UNKNOWN {
			continue
		}
		// extractor, found := tg.tableNames[dbOp.TableName]
		_, found := tg.tableNames[dbOp.TableName]
		if !found {
			continue
		}
		decodedDBOp, err := tg.abiCodec.DecodeDBOp(dbOp, gc.block.Number)
		if err != nil {
			return nil, err
		}
		// TODO manage key based on table name in tableNames => return a key generator function
		key := fmt.Sprintf("%s:%s", decodedDBOp.Scope, decodedDBOp.PrimaryKey)
		tableCamelCase, ceType := tableCeType(dbOp.TableName)
		ceId := hashString(fmt.Sprintf(
			"%s%s%d%d%s%s%s",
			gc.block.Id,
			gc.transaction.Id,
			gc.actionTrace.ExecutionIndex,
			dbOp.Operation,
			dbOp.TableName,
			gc.stepName,
			key))
		value := newTableNotification(
			notificationContextMap(gc),
			actionInfoBasicMap(gc),
			decodedDBOp.asMap(dbOpRecordName(tableCamelCase)),
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
			CeType: g.CeType,
			CeId:   g.CeId,
			Key:    g.Key,
			Value:  value,
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
	// TODO see if we need a defaulting on the key
	// key := fmt.Sprintf("%s:%d", gc.transaction.Id, gc.actionTrace.ExecutionIndex)
	key, err := evalString(extractor, activation)
	if err != nil {
		return nil, err
	}
	_, ceType := actionCeType(actionName)
	ceId := hashString(fmt.Sprintf(
		"%s%s%d%s%s%s",
		gc.block.Id,
		gc.transaction.Id,
		gc.actionTrace.ExecutionIndex,
		ceType,
		gc.stepName,
		key))
	jsonData := make(map[string]interface{})
	if stringData := gc.actionTrace.Action.JsonData; stringData != "" {
		err = json.Unmarshal(json.RawMessage(stringData), &jsonData)
		if err != nil {
			return nil, err
		}
	}
	value := newActionNotification(
		notificationContextMap(gc),
		newActionInfo(actionInfoBasicMap(gc), jsonData),
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
