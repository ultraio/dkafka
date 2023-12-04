package dkafka

import (
	"fmt"
	"strconv"
	"strings"

	pbcodec "github.com/pinax-network/firehose-antelope/types/pb/sf/antelope/type/v1"
	"github.com/google/cel-go/cel"
)

type Generation struct {
	Key          string         `json:"key,omitempty"`
	CeType       string         `json:"type,omitempty"`
	DecodedDBOps []*decodedDBOp `json:"db_ops,omitempty"`
}

type Generator interface {
	Apply(
		stepName string,
		transaction *pbcodec.TransactionTrace,
		trace *pbcodec.ActionTrace,
		decodedDBOps []*decodedDBOp,
	) ([]Generation, error)
}

type ActionConf struct {
	Filter string `json:"filter,omitempty"`
	First  string `json:"first,omitempty"`
	Split  bool   `json:"split,omitempty"`
	Key    string `json:"key"`
	CeType string `json:"type"`
}

type ActionsConf = map[string][]ActionConf

func NewActionsGenerator(config ActionsConf, skipKey ...bool) (Generator, error) {
	handlersByAction := make(map[string][]actionHandler)
	for actionName, actions := range config {
		actionHandlers := make([]actionHandler, 0, 1)
		for _, action := range actions {
			if action.Filter != "" && action.First != "" {
				return nil, fmt.Errorf("only one projection operator at a time per action handler is supported. Action:%s", actionName)
			}
			var proj projection
			if action.Filter != "" {
				matcher, err := expressionToMatcher(action.Filter)
				if err != nil {
					return nil, fmt.Errorf("invalid filter expression: %v, error: %w", action.Filter, err)
				}
				proj = filter{
					matcher: matcher,
				}
			} else if action.First != "" {
				matcher, err := expressionToMatcher(action.First)
				if err != nil {
					return nil, fmt.Errorf("invalid first expression: %v, error: %w", action.First, err)
				}
				proj = first{
					table: matcher,
				}
			} else {
				// indentity dbops projection
				proj = indentity{}
			}
			skipK := (len(skipKey) > 0 && skipKey[0])
			var keyExtractor cel.Program
			if action.Key != "" && !skipK {
				var err error
				keyExtractor, err = exprToCelProgram(action.Key)
				if err != nil {
					return nil, fmt.Errorf("action: %s, cannot parse key expression: %w", actionName, err)
				}
			} else if !skipK && action.Key == "" {
				return nil, fmt.Errorf("missing key expression for action: %s", actionName)
			}
			if action.CeType == "" {
				return nil, fmt.Errorf("missing type string for action: %s", actionName)
			}
			actionHandlers = append(actionHandlers, actionHandler{
				projection:   proj,
				ceType:       action.CeType,
				keyExtractor: keyExtractor,
				split:        action.Split,
			})
		}

		handlersByAction[actionName] = actionHandlers
	}
	return ActionGenerator{
		actions: handlersByAction,
	}, nil
}

var operationTypeByName map[string]pbcodec.DBOp_Operation = map[string]pbcodec.DBOp_Operation{
	"UNKNOWN": 0,
	"INSERT":  1,
	"UPDATE":  2,
	"DELETE":  3,
}

func stringAsDBOpOperation(s string) (op pbcodec.DBOp_Operation, err error) {
	if len(s) > 1 { // string representation of the operation
		var found bool
		op, found = operationTypeByName[strings.ToUpper(s)]
		if !found {
			err = fmt.Errorf("invalid operation matcher value must be one of: {UNKNOWN|INSERT|UPDATE|DELETE}, on: %s", s)
		}
	} else { // numerical representation of the operation [0..3]
		// ParseUint
		var op64 uint64
		op64, err = strconv.ParseUint(s, 10, 32)
		if err != nil || op64 > 3 {
			err = fmt.Errorf("invalid operation matcher value must be a uint between [0..3], on: %s, with error: %w", s, err)
		} else {
			op = pbcodec.DBOp_Operation(op64)
		}
	}
	return
}

func expressionToMatcher(expression string) (matcher, error) {
	tokens := strings.Split(expression, ":")
	if len(tokens) == 1 { // <- table name only short expression (legacy) => table-name
		return tableNameMatcher{tokens[0]}, nil
	} else if tokens[0] == "*" && tokens[1] == "*" { // <- invalid expression => *:*
		return nil, fmt.Errorf("unsupported table matcher expression: %s, you must at least fix on of them", expression)
	} else if tokens[0] == "*" { // <- table name only long expression => *:table-name
		return tableNameMatcher{tokens[1]}, nil
	} else if tokens[1] == "*" { // <- operation only expression => 1:*, INSERT:*
		op, err := stringAsDBOpOperation(tokens[0])
		if err != nil {
			return nil, err
		}
		return operationMatcher{op}, nil
	} else {
		op, err := stringAsDBOpOperation(tokens[0])
		if err != nil {
			return nil, err
		}
		return operationOnTableMatcher{op: pbcodec.DBOp_Operation(op), name: tokens[1]}, nil
	}
}

func NewExpressionsGenerator(
	eventKeyProg cel.Program,
	eventTypeProg cel.Program,
) Generator {
	return ExpressionsGenerator{
		keyExtractor:    eventKeyProg,
		ceTypeExtractor: eventTypeProg,
	}
}

type projection interface {
	on(decodedDBOps []*decodedDBOp) []*decodedDBOp
}

type first struct {
	table matcher
}

func (f first) on(decodedDBOps []*decodedDBOp) []*decodedDBOp {
	for _, dbOp := range decodedDBOps {
		if f.table.match(dbOp.DBOp) {
			return []*decodedDBOp{dbOp}
		}
	}
	return nil
}

type filter struct {
	matcher matcher
}

func (f filter) on(decodedDBOps []*decodedDBOp) []*decodedDBOp {
	var filteredDBOps []*decodedDBOp = make([]*decodedDBOp, 0, 1)
	for _, dbOp := range decodedDBOps {
		if f.matcher.match(dbOp.DBOp) {
			filteredDBOps = append(filteredDBOps, dbOp)
		}
	}
	return filteredDBOps
}

type matcher interface {
	match(value *pbcodec.DBOp) bool
}

type tableNameMatcher struct {
	name string
}

func (m tableNameMatcher) match(value *pbcodec.DBOp) bool {
	return m.name == value.TableName
}

type operationMatcher struct {
	op pbcodec.DBOp_Operation
}

func (m operationMatcher) match(value *pbcodec.DBOp) bool {
	return m.op == value.Operation
}

type operationOnTableMatcher struct {
	name string
	op   pbcodec.DBOp_Operation
}

func (m operationOnTableMatcher) match(value *pbcodec.DBOp) bool {
	return m.op == value.Operation && m.name == value.TableName
}

type indentity struct {
}

func (i indentity) on(decodedDBOps []*decodedDBOp) []*decodedDBOp {
	return decodedDBOps
}

type actionHandler struct {
	projection   projection
	keyExtractor cel.Program
	ceType       string
	split        bool
}

type ActionGenerator struct {
	actions map[string][]actionHandler
}

func (a actionHandler) evalGeneration(stepName string,
	transaction *pbcodec.TransactionTrace,
	trace *pbcodec.ActionTrace,
	ops []*decodedDBOp) (Generation, error) {
	activation, err := NewActivation(stepName, transaction,
		trace,
		ops,
	)
	if err != nil {
		return Generation{}, err
	}
	ceType := a.ceType

	key, err := evalString(a.keyExtractor, activation)
	if err != nil {
		return Generation{}, fmt.Errorf("action:%s, event keyeval: %w", trace.Action.Name, err)
	}

	return Generation{
		key,
		ceType,
		ops,
	}, nil
}

func (g ActionGenerator) Apply(stepName string,
	transaction *pbcodec.TransactionTrace,
	trace *pbcodec.ActionTrace,
	decodedDBOps []*decodedDBOp,
) ([]Generation, error) {
	if actionHandlers, ok := g.actions[trace.Action.Name]; ok {
		for _, actionHandler := range actionHandlers {
			projection := actionHandler.projection.on(decodedDBOps)
			if actionHandler.split {
				var generations []Generation = make([]Generation, 0, 1)
				for i := range projection {
					generation, err := actionHandler.evalGeneration(stepName, transaction, trace, projection[i:i+1])
					if err != nil {
						return nil, fmt.Errorf("actionHandler.evalGeneration() error: %w", err)
					}
					generations = append(generations, generation)
				}
				return generations, nil
			} else {
				generation, err := actionHandler.evalGeneration(stepName, transaction, trace, projection)
				if err != nil {
					return nil, fmt.Errorf("actionHandler.evalGeneration() error: %w", err)
				}
				return []Generation{generation}, nil
			}
		}
	}
	return nil, nil
}

type ExpressionsGenerator struct {
	keyExtractor    cel.Program
	ceTypeExtractor cel.Program
}

func (g ExpressionsGenerator) Apply(stepName string,
	transaction *pbcodec.TransactionTrace,
	trace *pbcodec.ActionTrace,
	decodedDBOps []*decodedDBOp,
) ([]Generation, error) {
	activation, err := NewActivation(stepName, transaction,
		trace,
		decodedDBOps,
	)
	if err != nil {
		return nil, err
	}

	eventType, err := evalString(g.ceTypeExtractor, activation)
	if err != nil {
		return nil, fmt.Errorf("error eventtype eval: %w", err)
	}

	eventKeys, err := evalStringArray(g.keyExtractor, activation)
	if err != nil {
		return nil, fmt.Errorf("event keyeval: %w", err)
	}
	var generations []Generation = make([]Generation, 0, 1)
	dedupeMap := make(map[string]bool)
	for _, eventKey := range eventKeys {
		if dedupeMap[eventKey] {
			continue
		}
		dedupeMap[eventKey] = true
		generations = append(generations, Generation{
			eventKey,
			eventType,
			decodedDBOps,
		})
	}
	return generations, nil
}
