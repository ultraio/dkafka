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

type TransactionContext struct {
	block       *pbcodec.Block
	stepName    string
	transaction *pbcodec.TransactionTrace
	blockStep   BlockStep
	cursor      string
	step        pbbstream.ForkStep
}

type ActionContext struct {
	TransactionContext
	correlation *Correlation
	actionTrace *pbcodec.ActionTrace
}

type GeneratorAtActionLevel interface {
	Apply(genContext ActionContext) ([]Generation2, error)
}

type GeneratorAtTransactionLevel interface {
	Apply(genContext TransactionContext) ([]*kafka.Message, error)
}

type Generation2 struct { // TODO rename to Message
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

type DecodeDBOp func(in *pbcodec.DBOp, blockNum uint32) (decodedDBOps *decodedDBOp, err error)

type ExtractKey func(*pbcodec.DBOp) string

// normalizeName return a 13 dotted string when the name value is empty otherwise it returns the given name
func normalizeName(name string) string {
	if name == "" {
		return "............."
	} else {
		return name
	}
}

func extractFullKey(dbOp *pbcodec.DBOp) string {
	return fmt.Sprintf("%s:%s", extractScope(dbOp), extractPrimaryKey(dbOp))
}

func extractScope(dbOp *pbcodec.DBOp) string {
	return normalizeName(dbOp.Scope)
}

func extractPrimaryKey(dbOp *pbcodec.DBOp) string {
	return normalizeName(dbOp.PrimaryKey)
}

func indexDbOps(gc ActionContext) []*IndexedEntry[*pbcodec.DBOp] {
	return orderSliceOnBlockStep(NewIndexedEntrySlice(gc.transaction.DBOpsForAction(gc.actionTrace.ExecutionIndex)), gc.step)
}

type TableKeyExtractorFinder func(string) (ExtractKey, bool)

type ActionKeyExtractorFinder func(string) (cel.Program, bool)

type TableGenerator struct {
	getExtractKey TableKeyExtractorFinder
	abiCodec      ABICodec
}

type void struct{}
type StringSet = map[string]void

func (tg TableGenerator) Apply(gc ActionContext) (generations []Generation2, err error) {
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

func (tg TableGenerator) doApply(gc ActionContext) ([]generation, error) {
	indexedDbOps := indexDbOps(gc)
	generations := []generation{}
	for _, indexedDbOp := range indexedDbOps {
		dbOp := indexedDbOp.Entry
		dbOpIndex := indexedDbOp.Index
		if dbOp.Operation == pbcodec.DBOp_OPERATION_UNKNOWN {
			continue
		}
		// extractor, found := tg.tableNames[dbOp.TableName]
		extractKey, found := tg.getExtractKey(dbOp.TableName)
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
	keyExtractors ActionKeyExtractorFinder
	abiCodec      ABICodec
}

func (ag ActionGenerator2) Apply(gc ActionContext) ([]Generation2, error) {
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
			return nil, err
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

func (ag ActionGenerator2) doApply(gc ActionContext) ([]generation, error) {
	actionName := gc.actionTrace.Action.Name
	extractor, found := ag.keyExtractors(actionName)
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

func notificationContextMap(gc ActionContext) map[string]interface{} {
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

func actionInfoBasicMap(gc ActionContext) map[string]interface{} {
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
		gc.actionTrace.ActionOrdinal,
		gc.actionTrace.CreatorActionOrdinal,
		gc.actionTrace.ClosestUnnotifiedAncestorActionOrdinal,
		gc.actionTrace.ExecutionIndex,
	)
}

type transaction2ActionsGenerator struct {
	actionLevelGenerator GeneratorAtActionLevel
	abiCodec             ABICodec
	headers              []kafka.Header
	topic                string
	account              string
}

func (t transaction2ActionsGenerator) isThisSmartContractABIUpdated(action *pbcodec.Action) bool {
	return action.Name == "setabi" && (t.isSelfAuthorized(action.Authorization) || t.isSetABIOnTrackedAccount(action))
}

func (t transaction2ActionsGenerator) isSetABIOnTrackedAccount(action *pbcodec.Action) bool {
	var actionParameters map[string]interface{}
	if err := json.Unmarshal(json.RawMessage(action.JsonData), &actionParameters); err != nil {
		return false
	}
	if account, found := actionParameters["account"]; found {
		return account == t.account
	}
	return false
}

func (t transaction2ActionsGenerator) isSelfAuthorized(authorizations []*pbcodec.PermissionLevel) bool {
	for _, permissionLevel := range authorizations {
		if permissionLevel.Actor == t.account {
			return true
		}
	}
	return false
}

func (t transaction2ActionsGenerator) Apply(genContext TransactionContext) ([]*kafka.Message, error) {
	msgs := make([]*kafka.Message, 0)
	// manage correlation
	trx := genContext.transaction
	// blkStep := genContext.step
	correlation, err := getCorrelation(trx.ActionTraces)
	if err != nil {
		return nil, err
	}
	acts := trx.ActionTraces
	zlog.Debug("adapt transaction", zap.Uint32("block_num", genContext.block.Number), zap.Int("trx_index", int(trx.Index)), zap.Int("nb_acts", len(acts)))
	for _, act := range orderSliceOnBlockStep(acts, genContext.step) {
		if !act.FilteringMatched {
			continue
		}
		if t.isThisSmartContractABIUpdated(act.Action) {
			zlog.Info("new abi published defer clear ABI cache at end of this block parsing", zap.Uint32("block_num", genContext.block.Number), zap.Int("trx_index", int(trx.Index)), zap.String("trx_id", trx.Id))
			t.abiCodec.UpdateABI(genContext.block.Number, genContext.step, trx.Id, act)
			continue
		}
		actionTracesReceived.Inc()
		// generation
		generations, err := t.actionLevelGenerator.Apply(ActionContext{
			TransactionContext: genContext,
			actionTrace:        act,
			correlation:        correlation,
		})

		if err != nil {
			zlog.Debug("fail fast on generator.Apply()", zap.Error(err))
			return nil, err
		}

		for _, generation := range generations {
			headers := append(t.headers,
				kafka.Header{
					Key:   "ce_id",
					Value: generation.CeId,
				},
				kafka.Header{
					Key:   "ce_type",
					Value: []byte(generation.CeType),
				},
				BlockStep{blk: genContext.block}.timeHeader(),
				kafka.Header{
					Key:   "ce_blkstep",
					Value: []byte(genContext.stepName),
				},
			)
			headers = append(headers, generation.Headers...)
			if correlation != nil {
				headers = append(headers,
					kafka.Header{
						Key:   "ce_parentid",
						Value: []byte(correlation.Id),
					},
				)
			}
			msg := &kafka.Message{
				Key:     []byte(generation.Key),
				Headers: headers,
				Value:   generation.Value,
				TopicPartition: kafka.TopicPartition{
					Topic:     &t.topic,
					Partition: kafka.PartitionAny,
				},
			}
			msgs = append(msgs, msg)
		}
	}
	return msgs, nil
}
