package dkafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

const transactionNotification = "TransactionNotification"
const transactionNamespace = "io.ultra.dkafka.transaction"

type transactionMetaSupplier struct {
}

func (dms transactionMetaSupplier) GetVersion() string {
	return "1.0.0"
}
func (dms transactionMetaSupplier) GetSource() string {
	return "dkafka-cli"
}
func (dms transactionMetaSupplier) GetDomain() string {
	return "dkafka"
}
func (dms transactionMetaSupplier) GetCompatibility() string {
	return "FORWARD"
}
func (dms transactionMetaSupplier) GetType() string {
	return "notification"
}

var TransactionMessageSchema = MessageSchema{
	transactionSchema,
	newMeta(transactionMetaSupplier{}),
}

type TransactionReceiptHeaderBasicSchema = RecordSchema
type ActionTraceBasicSchema = RecordSchema
type ActionBasicSchema = RecordSchema
type PermissionLevelBasicSchema = RecordSchema
type AccountRamDeltasBasicSchema = RecordSchema
type ExceptionBasicSchema = RecordSchema
type RAMOpBasicSchema = RecordSchema
type CreationTreeBasicSchema = RecordSchema

func Map[T, U any](ts []T, f func(T) U) []U {
	us := make([]U, len(ts))
	for i := range ts {
		us[i] = f(ts[i])
	}
	return us
}

func newTransactionReceiptHeaderBasicSchema() TransactionReceiptHeaderBasicSchema {
	return newRecordS(
		"TransactionReceiptHeader",
		[]FieldSchema{
			NewOptionalField("status", "int"),
			NewOptionalField("cpu_usage_micro_seconds", "long"),
		},
	)
}

func newActionTraceBasicSchema() ActionTraceBasicSchema {
	return newRecordS(
		"ActionTrace",
		[]FieldSchema{
			NewOptionalField("receiver", "string"),
			NewOptionalField("action", newActionBasicSchema()),
			NewOptionalField("context_free", "boolean"),
			NewOptionalField("elapsed", "long"),
			{Name: "account_ram_deltas", Type: NewArray(newAccountRamDeltasBasicSchema())},
			NewOptionalField("exception", newExceptionBasicSchema()),
			NewOptionalField("error_code", NewUint64Type()),
			NewOptionalField("action_ordinal", "int"),
			NewOptionalField("creator_action_ordinal", "int"),
			NewOptionalField("closest_unnotified_ancestor_action_ordinal", "int"),
			NewOptionalField("execution_index", "int"),
		},
	)
}

func newActionBasicSchema() ActionBasicSchema {
	return newRecordS(
		"Action",
		[]FieldSchema{
			NewOptionalField("account", "string"),
			NewOptionalField("name", "string"),
			{Name: "authorization", Type: NewArray(newPermissionLevelBasicSchema())},
		},
	)
}
func newPermissionLevelBasicSchema() PermissionLevelBasicSchema {
	return newRecordS(
		"PermissionLevel",
		[]FieldSchema{
			NewOptionalField("actor", "string"),
			NewOptionalField("permission", "string"),
		},
	)
}

func newAccountRamDeltasBasicSchema() AccountRamDeltasBasicSchema {
	return newRecordS(
		"AccountRamDelta",
		[]FieldSchema{
			NewOptionalField("account", "string"),
			NewOptionalField("delta", "long"),
		},
	)
}

func newExceptionBasicSchema() ExceptionBasicSchema {
	return newRecordS(
		"Exception",
		[]FieldSchema{
			NewOptionalField("code", "int"),
			NewOptionalField("name", "string"),
			NewOptionalField("message", "string"),
		},
	)
}

func newRAMOpBasicSchema() RAMOpBasicSchema {
	return newRecordS(
		"RAMOp",
		[]FieldSchema{
			NewOptionalField("operation", "int"),
			NewOptionalField("action_index", "long"),
			NewOptionalField("payer", "string"),
			NewOptionalField("delta", "long"),
			NewOptionalField("usage", "long"),
		},
	)
}

func newCreationTreeBasicSchema() CreationTreeBasicSchema {
	return newRecordS(
		"CreationFlatNode",
		[]FieldSchema{
			NewOptionalField("creator_action_index", "long"),
			NewOptionalField("execution_action_index", "long"),
		},
	)
}

var transactionSchema = RecordSchema{
	Type:      "record",
	Name:      transactionNotification,
	Namespace: transactionNamespace,
	Doc:       "Transaction info",
	Fields: []FieldSchema{
		NewOptionalField("id", "string"),
		NewOptionalField("block_num", "long"),
		NewOptionalField("index", "long"),
		NewOptionalField("block_time", NewTimestampMillisType("block_timestamp_type")),
		NewOptionalField("producer_block_id", "string"),
		NewOptionalField("receipt", newTransactionReceiptHeaderBasicSchema()),
		NewOptionalField("elapsed", "long"),
		NewOptionalField("net_usage", "long"),
		{Name: "action_traces", Type: NewArray(newActionTraceBasicSchema())},
		NewOptionalField("exception", "Exception"),
		NewOptionalField("error_code", NewUint64Type()),
		{Name: "ram_ops", Type: NewArray(newRAMOpBasicSchema())},
		{Name: "creation_tree", Type: NewArray(newCreationTreeBasicSchema())},
	},
}

func newTransactionMap(transaction *pbcodec.TransactionTrace) map[string]interface{} {
	return map[string]interface{}{
		"id":                transaction.Id,
		"block_num":         transaction.BlockNum,
		"index":             transaction.Index,
		"block_time":        transaction.BlockTime.AsTime(),
		"producer_block_id": transaction.ProducerBlockId,
		"receipt":           newTransactionReceiptHeaderMap(transaction.Receipt),
		"elapsed":           transaction.Elapsed,
		"net_usage":         transaction.NetUsage,
		"action_traces":     newActionTracesMap(transaction.ActionTraces),
		"exception":         newExceptionMap(transaction.Exception),
		"error_code":        transaction.ErrorCode,
		"ram_ops":           newRAMOpsMap(transaction.RamOps),
		"creation_tree":     newCreationTreeMap(transaction.CreationTree),
	}
}

func newTransactionReceiptHeaderMap(transactionReceiptHeader *pbcodec.TransactionReceiptHeader) map[string]interface{} {
	if transactionReceiptHeader != nil {
		return map[string]interface{}{
			"status":                  transactionReceiptHeader.Status,
			"cpu_usage_micro_seconds": transactionReceiptHeader.CpuUsageMicroSeconds,
		}
	} else {
		return nil
	}
}

func newActionTracesMap(actionTraces []*pbcodec.ActionTrace) []map[string]interface{} {
	return Map(actionTraces, newActionTraceMap)
}

func newActionTraceMap(actionTrace *pbcodec.ActionTrace) map[string]interface{} {
	if actionTrace != nil {
		return map[string]interface{}{
			"receiver":               actionTrace.Receiver,
			"action":                 newActionMap(actionTrace.Action),
			"context_free":           actionTrace.ContextFree,
			"elapsed":                actionTrace.Elapsed,
			"account_ram_deltas":     newAccountRamDeltasMap(actionTrace.AccountRamDeltas),
			"exception":              newExceptionMap(actionTrace.Exception),
			"error_code":             actionTrace.ErrorCode,
			"action_ordinal":         actionTrace.ActionOrdinal,
			"creator_action_ordinal": actionTrace.CreatorActionOrdinal,
			"closest_unnotified_ancestor_action_ordinal": actionTrace.ClosestUnnotifiedAncestorActionOrdinal,
			"execution_index": actionTrace.ExecutionIndex,
		}
	} else {
		return nil
	}
}

func newActionMap(action *pbcodec.Action) map[string]interface{} {
	if action != nil {
		return map[string]interface{}{
			"account":       action.Account,
			"name":          action.Name,
			"authorization": newAuthorizationsMap(action.Authorization),
		}
	} else {
		return nil
	}
}

func newAuthorizationsMap(exceptions []*pbcodec.PermissionLevel) []map[string]interface{} {
	return Map(exceptions, newAuthorizationMap)
}

func newAuthorizationMap(exception *pbcodec.PermissionLevel) map[string]interface{} {
	if exception != nil {
		return map[string]interface{}{
			"actor":      exception.Actor,
			"permission": exception.Permission,
		}
	} else {
		return nil
	}
}

func newAccountRamDeltasMap(accountRAMDeltas []*pbcodec.AccountRAMDelta) []map[string]interface{} {
	return Map(accountRAMDeltas, newAccountRamDeltaMap)
}

func newAccountRamDeltaMap(accountRAMDelta *pbcodec.AccountRAMDelta) map[string]interface{} {
	if accountRAMDelta != nil {
		return map[string]interface{}{
			"account": accountRAMDelta.Account,
			"delta":   accountRAMDelta.Delta,
		}
	} else {
		return nil
	}
}

func newExceptionMap(exception *pbcodec.Exception) map[string]interface{} {
	if exception != nil {
		return map[string]interface{}{
			"code":    exception.Code,
			"name":    exception.Name,
			"message": exception.Message,
		}
	} else {
		return nil
	}
}

func newRAMOpsMap(rampOp []*pbcodec.RAMOp) []map[string]interface{} {
	return Map(rampOp, newRAMOpMap)
}

func newRAMOpMap(rampOp *pbcodec.RAMOp) map[string]interface{} {
	if rampOp != nil {
		return map[string]interface{}{
			"operation":    rampOp.Operation,
			"action_index": rampOp.ActionIndex,
			"payer":        rampOp.Payer,
			"delta":        rampOp.Delta,
			"usage":        rampOp.Usage,
		}
	} else {
		return nil
	}
}

func newCreationTreeMap(creationFlatNodes []*pbcodec.CreationFlatNode) []map[string]interface{} {
	return Map(creationFlatNodes, newCreationNodeMap)
}

func newCreationNodeMap(creationFlatNode *pbcodec.CreationFlatNode) map[string]interface{} {
	if creationFlatNode != nil {
		return map[string]interface{}{
			"creator_action_index":   creationFlatNode.CreatorActionIndex,
			"execution_action_index": creationFlatNode.ExecutionActionIndex,
		}
	} else {
		return nil
	}
}

type transactionGenerator struct {
	headers  []kafka.Header
	topic    string
	abiCodec ABICodec
}

func (t transactionGenerator) Apply(genContext TransactionContext) ([]*kafka.Message, error) {

	transactionMap := newTransactionMap(genContext.transaction)
	codec, err := t.abiCodec.GetCodec(transactionNotification, 0)
	if err != nil {
		return nil, fmt.Errorf("transactionGenerator.Apply() fail to get codec for %s: %w", transactionNotification, err)
	}
	value, err := codec.Marshal(nil, transactionMap)
	if err != nil {
		return nil, fmt.Errorf("transactionGenerator.Apply() fail to marshal %s: %w", dkafkaCheckpoint, err)
	}
	transactionIdBytes := []byte(genContext.transaction.Id)
	headers := append(t.headers,
		kafka.Header{
			Key:   "ce_id",
			Value: transactionIdBytes,
		},
		kafka.Header{
			Key:   "ce_type",
			Value: []byte(transactionNotification),
		},
		kafka.Header{
			Key:   "ce_blkstep",
			Value: []byte(genContext.stepName),
		},
		genContext.blockStep.timeHeader(),
		newCursorHeader(genContext.cursor),
		newPreviousCursorHeader(genContext.blockStep.previousCursor),
	)
	headers = append(headers, codec.GetHeaders()...)

	msg := &kafka.Message{
		Key:     transactionIdBytes,
		Headers: headers,
		Value:   value,
		TopicPartition: kafka.TopicPartition{
			Topic:     &t.topic,
			Partition: kafka.PartitionAny,
		},
	}
	return []*kafka.Message{msg}, nil
}
