package dkafka

import (
	"encoding/json"
	"time"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

func newCorrelationRecord() RecordSchema {
	return newRecordFQN(
		dkafkaNamespace,
		"Correlation",
		[]FieldSchema{
			{
				Name: "payer",
				Type: "string",
			},
			{
				Name: "id",
				Type: "string",
			},
		},
	)
}

func newOptionalCorrelation(correlation *Correlation) map[string]interface{} {
	if correlation != nil {
		return newCorrelation(correlation.Payer, correlation.Id)
	} else {
		return nil
	}
}

func newCorrelation(payer string, id string) map[string]interface{} {
	return map[string]interface{}{
		"payer": payer,
		"id":    id,
	}
}

type Correlation struct {
	Payer string `json:"payer"`
	Id    string `json:"id"`
}

func newDBOpBasic(dbOp *pbcodec.DBOp, dbOpIndex int) map[string]interface{} {
	asMap := map[string]interface{}{
		"operation":    int32(dbOp.Operation),
		"action_index": dbOp.ActionIndex,
		"index":        dbOpIndex,
	}
	addOptionalString(&asMap, "code", dbOp.Code)
	addOptionalString(&asMap, "scope", dbOp.Scope)
	addOptionalString(&asMap, "table_name", dbOp.TableName)
	addOptionalString(&asMap, "primary_key", dbOp.PrimaryKey)
	addOptionalString(&asMap, "old_payer", dbOp.OldPayer)
	addOptionalString(&asMap, "new_payer", dbOp.NewPayer)
	addOptionalBytes(&asMap, "old_data", dbOp.OldData)
	addOptionalBytes(&asMap, "new_data", dbOp.NewData)
	return asMap
}

func addOptionalBytes(m *map[string]interface{}, key string, value []byte) {
	if len(value) > 0 {
		(*m)[key] = value
	}
}

func addOptionalString(m *map[string]interface{}, key string, value string) {
	if value != "" {
		(*m)[key] = value
	}
}
func newDBOpBasicSchema() DBOpBasicSchema {
	return newRecordFQN(
		dkafkaNamespace,
		"DBOpBasic",
		[]FieldSchema{
			NewOptionalField("operation", "int"),
			NewOptionalField("action_index", "long"),
			NewIntField("index"),
			NewOptionalField("code", "string"),
			NewOptionalField("scope", "string"),
			NewOptionalField("table_name", "string"),
			NewOptionalField("primary_key", "string"),
			NewOptionalField("old_payer", "string"),
			NewOptionalField("new_payer", "string"),
			NewOptionalField("old_data", "bytes"),
			NewOptionalField("new_data", "bytes"),
		},
	)
}

func newDBOpInfoRecord(tableName string, jsonData RecordSchema) RecordSchema {
	result := newDBOpBasicSchema()
	result.Name = tableName
	result.Namespace = ""
	result.Fields = append(result.Fields,
		NewOptionalField("old_json", jsonData),
		NewOptionalField("new_json", jsonData.Name),
	)
	return result
}

func newActionInfoBasic(
	account string,
	receiver string,
	name string,
	globalSequence uint64,
	authorization []string,
) map[string]interface{} {
	return map[string]interface{}{
		"account":        account,
		"receiver":       receiver,
		"name":           name,
		"global_seq":     globalSequence,
		"authorizations": authorization,
	}
}

type ActionInfoBasicSchema = RecordSchema
type DBOpBasicSchema = RecordSchema

func newActionInfoBasicSchema() ActionInfoBasicSchema {
	return newActionInfoBasicSchemaFQN("ActionInfoBasic", dkafkaNamespace)
}
func newActionInfoBasicSchemaN(name string) ActionInfoBasicSchema {
	return newActionInfoBasicSchemaFQN(name, "")
}
func newActionInfoBasicSchemaFQN(name string, np string) ActionInfoBasicSchema {
	return newRecordFQN(
		np,
		name,
		[]FieldSchema{
			{
				Name: "account",
				Type: "string",
			},
			{
				Name: "receiver",
				Type: "string",
			},
			{
				Name: "name",
				Type: "string",
			},
			{
				Name: "global_seq",
				Type: "long",
			},
			{
				Name: "authorizations",
				Type: NewArray("string"),
			},
		},
	)
}

type ActionInfoDetails struct {
	// todo inherit ActionInfoBasic
	Account        string           `json:"account"`
	Receiver       string           `json:"receiver"`
	Action         string           `json:"action"`
	GlobalSequence uint64           `json:"global_seq"`
	Authorization  []string         `json:"authorizations"`
	DBOps          []*decodedDBOp   `json:"db_ops"`
	JSONData       *json.RawMessage `json:"json_data"`
}

type event struct {
	BlockNum      uint32            `json:"block_num"`
	BlockID       string            `json:"block_id"`
	Status        string            `json:"status"`
	Executed      bool              `json:"executed"`
	Step          string            `json:"block_step"`
	Correlation   *Correlation      `json:"correlation,omitempty"`
	TransactionID string            `json:"trx_id"`
	ActionInfo    ActionInfoDetails `json:"act_info"`
}

func (e event) JSON() []byte {
	b, _ := json.Marshal(e)
	return b

}

func newNotificationContext(
	blockID string,
	blockNum uint32,
	status string,
	executed bool,
	step string,
	transactionID string,
	correlation map[string]interface{},
	time time.Time,
	cursor string,
) map[string]interface{} {
	nc := map[string]interface{}{
		"block_id":   blockID,
		"block_num":  blockNum,
		"status":     status,
		"executed":   executed,
		"block_step": step,
		"trx_id":     transactionID,
		"time":       time,
		"cursor":     cursor,
	}
	if len(correlation) > 0 {
		nc["correlation"] = correlation
	}
	return nc
}

type NotificationContextSchema = RecordSchema

func newNotificationContextSchema() NotificationContextSchema {
	return newRecordFQN(
		dkafkaNamespace,
		"NotificationContext",
		[]FieldSchema{
			{
				Name: "block_num",
				Type: "long",
			},
			{
				Name: "block_id",
				Type: "string",
			},
			{
				Name: "status",
				Type: "string",
			},
			{
				Name: "executed",
				Type: "boolean",
			},
			{
				Name: "block_step",
				Type: "string",
			},
			NewOptionalField("correlation", newCorrelationRecord()),
			{
				Name: "trx_id",
				Type: "string",
			},
			NewTimestampMillisField("time"),
			{
				Name: "cursor",
				Type: "string",
			},
		},
	)
}

func newTableNotification(context map[string]interface{}, action map[string]interface{}, dbOp map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"context": context,
		"action":  action,
		"db_op":   dbOp,
	}
}

func newTableNotificationSchema(name string, namespace string, ms MetaSupplier, dbOpRecord RecordSchema) MessageSchema {
	record := newRecordFQN(
		namespace,
		name,
		[]FieldSchema{
			{
				Name: "context",
				Type: newNotificationContextSchema(),
			},
			{
				Name: "action",
				Type: newActionInfoBasicSchema(),
			},
			{
				Name: "db_op",
				Type: dbOpRecord,
			},
		},
	)
	return MessageSchema{
		record,
		newMeta(ms),
	}
}

func newActionNotification(context map[string]interface{}, actionInfo map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"context":  context,
		"act_info": actionInfo,
	}
}

func newActionNotificationSchema(name string, namespace string, ms MetaSupplier, actionInfoSchema ActionInfoSchema) MessageSchema {
	record := newRecordFQN(
		namespace,
		name,
		[]FieldSchema{
			{
				Name: "context",
				Type: newNotificationContextSchema(),
			},
			{
				Name: "act_info",
				Type: actionInfoSchema,
			},
		},
	)
	return MessageSchema{
		record,
		newMeta(ms),
	}
}

func newActionInfo(
	actionInfoBasic map[string]interface{},
	jsonData map[string]interface{},
	dbOps []map[string]interface{},
) map[string]interface{} {
	actionInfoBasic["json_data"] = jsonData
	actionInfoBasic["db_ops"] = dbOps
	return actionInfoBasic
}

type ActionInfoSchema = RecordSchema

func newActionInfoSchema(name string, jsonData RecordSchema) ActionInfoSchema {
	result := newActionInfoBasicSchemaN(name)
	result.Fields = append(result.Fields,
		FieldSchema{Name: "json_data",
			Type: jsonData,
		},
		FieldSchema{Name: "db_ops",
			Type: NewArray(newDBOpBasicSchema()),
		},
	)
	return result
}
