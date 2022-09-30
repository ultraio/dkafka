package dkafka

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/interpreter"
	"go.uber.org/zap"
)

var Declarations = cel.Declarations(
	decls.NewVar("receiver", decls.String), // eosio.account_name::receiver
	decls.NewVar("account", decls.String),  // eosio.account_name::account
	decls.NewVar("action", decls.String),   // eosio.name::action

	decls.NewVar("block_num", decls.Uint),    // uint32 block number
	decls.NewVar("block_id", decls.String),   // string block id (hash)
	decls.NewVar("block_time", decls.String), // string timestamp

	decls.NewVar("step", decls.String),            // one of: Irreversible, New, Undo, Redo, Unknown
	decls.NewVar("transaction_id", decls.String),  // string transaction id (hash)
	decls.NewVar("transaction_index", decls.Uint), // uint transaction position inside the block
	decls.NewVar("global_seq", decls.Uint),        // uint
	decls.NewVar("execution_index", decls.Uint),   // uint action position inside the transaction

	decls.NewVar("data", decls.NewMapType(decls.String, decls.Any)),
	decls.NewVar("auth", decls.NewListType(decls.String)),

	decls.NewVar("db_ops", decls.NewListType(decls.NewMapType(decls.String, decls.Any))),
)

func NewActivation(stepName string, transaction *pbcodec.TransactionTrace, trace *pbcodec.ActionTrace, decodedDBOps []*decodedDBOp) (interpreter.Activation, error) {
	var activationMap = map[string]interface{}{
		"block_num":         transaction.BlockNum,
		"block_id":          transaction.ProducerBlockId,
		"block_time":        func() interface{} { return transaction.BlockTime.AsTime().UTC().Format(time.RFC3339) },
		"transaction_id":    transaction.Id,
		"transaction_index": transaction.Index,
		"step":              stepName,
		"global_seq": func() interface{} {
			if trace.Receipt != nil {
				return trace.Receipt.GlobalSequence
			}
			return 0
		},
		"execution_index": trace.ExecutionIndex,
		"receiver": func() interface{} {
			if trace.Receipt != nil {
				return trace.Receipt.Receiver
			}
			return trace.Receiver
		},
		"account": trace.Account(),
		"action":  trace.Name(),
		"auth": func() interface{} {
			return tokenizeEOSAuthority(trace.Action.Authorization)
		},
		"data": func() interface{} {
			jsonData := trace.Action.JsonData
			if len(jsonData) == 0 || strings.IndexByte(jsonData, '{') == -1 {
				return nil
			}

			return jsonStringToMap(jsonData)
		},
		"db_ops": func() interface{} {
			if trace.Receipt == nil {
				return nil
			}
			// maybe we can have a look to https://github.com/mitchellh/mapstructure/blob/master/mapstructure.go
			if rawJSON, err := json.Marshal(decodedDBOps); err != nil {
				zlog.Error("invalid dpOps", zap.Error(err), zap.Uint64("globalSequence", trace.Receipt.GlobalSequence))
			} else {
				return rawJsonToMap(rawJSON)
			}
			return nil
		},
	}
	return interpreter.NewActivation(activationMap)
}

var TableDeclarations = cel.Declarations(
	decls.NewVar("receiver", decls.String), // eosio.account_name::receiver
	decls.NewVar("account", decls.String),  // eosio.account_name::account
	decls.NewVar("action", decls.String),   // eosio.name::action

	decls.NewVar("block_num", decls.Uint),  // uint32 block number
	decls.NewVar("block_id", decls.String), // string block id (hash)

	decls.NewVar("step", decls.String),            // one of: Irreversible, New, Undo, Redo, Unknown
	decls.NewVar("transaction_id", decls.String),  // string transaction id (hash)
	decls.NewVar("transaction_index", decls.Uint), // uint transaction position inside the block
	decls.NewVar("global_seq", decls.Uint),        // uint
	decls.NewVar("execution_index", decls.Uint),   // uint action position inside the transaction

	decls.NewVar("db_op", decls.NewMapType(decls.String, decls.Any)),
	decls.NewVar("table", decls.NewMapType(decls.String, decls.Any)),
)

func NewTableActivation(stepName string, transaction *pbcodec.TransactionTrace, trace *pbcodec.ActionTrace, decodedDBOp *decodedDBOp) (interpreter.Activation, error) {
	var activationMap = map[string]interface{}{
		"block_num":         transaction.BlockNum,
		"block_id":          transaction.ProducerBlockId,
		"transaction_id":    transaction.Id,
		"transaction_index": transaction.Index,
		"step":              stepName,
		"global_seq": func() interface{} {
			if trace.Receipt != nil {
				return trace.Receipt.GlobalSequence
			}
			return 0
		},
		"execution_index": trace.ExecutionIndex,
		"receiver": func() interface{} {
			if trace.Receipt != nil {
				return trace.Receipt.Receiver
			}
			return trace.Receiver
		},
		"account": trace.Account(),
		"action":  trace.Name(),
		"db_op": func() interface{} {
			if trace.Receipt == nil {
				return nil
			}
			// maybe we can have a look to https://github.com/mitchellh/mapstructure/blob/master/mapstructure.go
			if rawJSON, err := json.Marshal(decodedDBOp); err != nil {
				zlog.Fatal("invalid dpOp", zap.Error(err), zap.Uint64("globalSequence", trace.Receipt.GlobalSequence))
			} else {
				return rawJsonToMap(rawJSON)
			}
			return nil
		},
		"table": func() interface{} {
			if trace.Receipt == nil {
				return nil
			}
			switch op := decodedDBOp.Operation; op {
			case pbcodec.DBOp_OPERATION_INSERT:
				if rawJSON, err := json.Marshal(decodedDBOp.NewJSON); err != nil {
					zlog.Fatal("invalid dpOp", zap.Error(err), zap.Uint64("globalSequence", trace.Receipt.GlobalSequence))
				} else {
					return rawJsonToMap(rawJSON)
				}
			case pbcodec.DBOp_OPERATION_UPDATE:
				if rawJSON, err := json.Marshal(decodedDBOp.NewJSON); err != nil {
					zlog.Fatal("invalid dpOp", zap.Error(err), zap.Uint64("globalSequence", trace.Receipt.GlobalSequence))
				} else {
					return rawJsonToMap(rawJSON)
				}
			case pbcodec.DBOp_OPERATION_REMOVE:
				if rawJSON, err := json.Marshal(decodedDBOp.OldJSON); err != nil {
					zlog.Fatal("invalid dpOp", zap.Error(err), zap.Uint64("globalSequence", trace.Receipt.GlobalSequence))
				} else {
					return rawJsonToMap(rawJSON)
				}
			default:
				zlog.Fatal("unsupported operation type", zap.Int32("operation", int32(op)))
			}
			return nil
		},
	}
	return interpreter.NewActivation(activationMap)
}

var ActionDeclarations = cel.Declarations(
	decls.NewVar("receiver", decls.String), // eosio.account_name::receiver
	decls.NewVar("account", decls.String),  // eosio.account_name::account
	decls.NewVar("action", decls.String),   // eosio.name::action

	decls.NewVar("block_num", decls.Uint),    // uint32 block number
	decls.NewVar("block_id", decls.String),   // string block id (hash)
	decls.NewVar("block_time", decls.String), // string timestamp

	decls.NewVar("step", decls.String),            // one of: Irreversible, New, Undo, Redo, Unknown
	decls.NewVar("transaction_id", decls.String),  // string transaction id (hash)
	decls.NewVar("transaction_index", decls.Uint), // uint transaction position inside the block
	decls.NewVar("global_seq", decls.Uint),        // uint
	decls.NewVar("execution_index", decls.Uint),   // uint action position inside the transaction

	decls.NewVar("data", decls.NewMapType(decls.String, decls.Any)),
	decls.NewVar("auth", decls.NewListType(decls.String)),
)

func NewActionActivation(stepName string, transaction *pbcodec.TransactionTrace, trace *pbcodec.ActionTrace) (interpreter.Activation, error) {
	var activationMap = map[string]interface{}{
		"block_num":         transaction.BlockNum,
		"block_id":          transaction.ProducerBlockId,
		"block_time":        func() interface{} { return transaction.BlockTime.AsTime().UTC().Format(time.RFC3339) },
		"transaction_id":    transaction.Id,
		"transaction_index": transaction.Index,
		"step":              stepName,
		"global_seq": func() interface{} {
			if trace.Receipt != nil {
				return trace.Receipt.GlobalSequence
			}
			return 0
		},
		"execution_index": trace.ExecutionIndex,
		"receiver": func() interface{} {
			if trace.Receipt != nil {
				return trace.Receipt.Receiver
			}
			return trace.Receiver
		},
		"account": trace.Account(),
		"action":  trace.Name(),
		"auth": func() interface{} {
			return tokenizeEOSAuthority(trace.Action.Authorization)
		},
		"data": func() interface{} {
			jsonData := trace.Action.JsonData
			if len(jsonData) == 0 || strings.IndexByte(jsonData, '{') == -1 {
				return nil
			}

			return jsonStringToMap(jsonData)
		},
	}
	return interpreter.NewActivation(activationMap)
}

func jsonStringToMap(jsonData string) (out map[string]interface{}) {
	var rawJSON = json.RawMessage(jsonData)
	err := json.Unmarshal(rawJSON, &out)
	if err != nil {
		zlog.Error("invalid json data", zap.Error(err), zap.String("json", jsonData))

	}
	return
}

func rawJsonToMap(rawJSON json.RawMessage) (out []map[string]interface{}) {
	err := json.Unmarshal(rawJSON, &out)
	if err != nil {
		zlog.Error("invalid json data", zap.Error(err), zap.String("json", string(rawJSON)))
	}
	return
}

var stringType = reflect.TypeOf("")
var stringArrayType = reflect.TypeOf([]string{})

func evalString(prog cel.Program, activation interface{}) (string, error) {
	res, _, err := prog.Eval(activation)
	if err != nil {
		return "", err
	}
	out, err := res.ConvertToNative(stringType)
	if err != nil {
		return "", err
	}
	return out.(string), nil
}

func evalStringArray(prog cel.Program, activation interface{}) ([]string, error) {
	res, _, err := prog.Eval(activation)
	if err != nil {
		return nil, err
	}
	out, err := res.ConvertToNative(stringArrayType)
	if err != nil {
		return nil, err
	}
	return out.([]string), nil
}

func exprToCelProgram(stripped string) (prog cel.Program, err error) {
	return exprToCelProgramWithEnv(stripped, Declarations)
}

func exprToCelProgramWithEnv(stripped string, declarations cel.EnvOption) (prog cel.Program, err error) {
	env, err := cel.NewEnv(declarations)
	if err != nil {
		return nil, fmt.Errorf("creating new CEL environment: %w", err)
	}

	exprAst, issues := env.Compile(stripped)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compiling AST expression %s: %w", stripped, issues.Err())
	}

	prog, err = env.Program(exprAst)
	if err != nil {
		return nil, fmt.Errorf("creating program from AST expression %s: %w", stripped, err)
	}
	return
}

// This must follow rules taken in `search/tokenization.go`, ideally we would share this, maybe would be a good idea to
// put the logic in an helper method on type `pbcodec.PermissionLevel` directly.
func tokenizeEOSAuthority(authorizations []*pbcodec.PermissionLevel) (out []string) {
	out = make([]string, len(authorizations)*2)
	for i, auth := range authorizations {
		out[i*2] = auth.Actor
		out[i*2+1] = auth.Authorization()
	}
	return
}
