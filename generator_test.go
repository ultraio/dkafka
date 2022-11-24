package dkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/google/cel-go/cel"
)

func Test_NewActionGenerator(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		skipKey bool
		want    Generator
		wantErr bool
	}{
		{
			name: "filter-split",
			args: `{"buy":[{"split": true, "filter":"table1","key":"db_ops[0].table_name", "type":"TestEvent"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {actionHandler{
						projection: filter{
							matcher: tableNameMatcher{"table1"},
						},
						ceType: "TestEvent",
						split:  true,
					}},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name: "filter",
			args: `{"buy":[{"filter":"table1","key":"db_ops[0].table_name", "type":"TestEvent"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {actionHandler{
						projection: filter{
							matcher: tableNameMatcher{"table1"},
						},
						ceType: "TestEvent",
					}},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name: "identity",
			args: `{"buy":[{"key":"db_ops[0].table_name", "type":"TestEvent"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {actionHandler{
						projection: indentity{},
						ceType:     "TestEvent",
					}},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name: "multi-projections",
			args: `{"buy":[{"filter":"table1","key":"db_ops[0].table_name", "type":"TestEvent1"},{"filter":"table3","split": true,"key":"db_ops[0].table_name", "type":"TestEvent2"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {
						actionHandler{
							projection: filter{
								matcher: tableNameMatcher{"table1"},
							},
							ceType: "TestEvent1",
						},
						actionHandler{
							projection: filter{
								matcher: tableNameMatcher{"table3"},
							},
							ceType: "TestEvent2",
							split:  true,
						},
					},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name: "multi-actions",
			args: `{"buy":[{"filter":"table2","key":"db_ops[0].table_name", "type":"TestEvent1"}],"issue":[{"filter":"table3", "split": true,"key":"db_ops[0].table_name", "type":"TestEvent2"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {
						actionHandler{
							projection: filter{
								matcher: tableNameMatcher{"table2"},
							},
							ceType: "TestEvent1",
						},
					},
					"issue": {
						actionHandler{
							projection: filter{
								matcher: tableNameMatcher{"table3"},
							},
							ceType: "TestEvent2",
							split:  true,
						},
					},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name:    "missing-key",
			args:    `{"buy":[{"filter":"table1", "type":"TestEvent"}]}`,
			skipKey: false,
			wantErr: true,
		},
		{
			name:    "missing-type",
			args:    `{"buy":[{"filter":"table1","key":"db_ops[0].table_name"}]}`,
			skipKey: false,
			wantErr: true,
		},
		{
			name:    "too-many-operations",
			args:    `{"buy":[{"filter":"table1","first":"table2","key":"db_ops[0].table_name", "type":"TestEvent"}]}`,
			skipKey: false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actionsConf := actionsConfFromJSON(t, tt.args)
			if actionsConf == nil {
				return
			}
			got, err := NewActionsGenerator(actionsConf, tt.skipKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewActionGenerator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewActionGenerator() = %v, want %v", got, tt.want)
			}
		})
	}
}

func toJSON(v interface{}) json.RawMessage {
	json, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("json.Marshal() error: %v, on: %v", err, v))
	}
	return json
}

func actionsConfFromJSON(t *testing.T, config string) ActionsConf {
	actionsConf := make(ActionsConf)
	err := json.Unmarshal(json.RawMessage(config), &actionsConf)
	if err != nil {
		t.Fatalf("Unmarshal error = %v, config: %s", err, config)
		return nil
	}
	return actionsConf
}

func Test_NewExpressionsGenerator(t *testing.T) {
	t.Run("creation", func(t *testing.T) {
		got := NewExpressionsGenerator(compileExpr("action"), compileExpr("action"))
		_, ok := got.(ExpressionsGenerator)
		if !ok {
			t.Errorf("NewExpressionsGenerator() does not return a ExpressionsGenerator = %v", got)
		}
	})

}

func compileExpr(expr string) cel.Program {
	program, err := exprToCelProgram(expr)
	if err != nil {
		panic(fmt.Sprintf("exprToCelProgram error: %v, expr: %s", err, expr))
	}
	return program
}

func Test_ActionGenerator_Apply(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		config  string
		want    []Generation
		wantErr bool
	}{
		{
			"indentity",
			"testdata/block-30080032.json",
			`{"create":[{"key":"transaction_id", "type":"TestType"}]}`,
			[]Generation{
				{
					Key:          "a2a53dce154c2ccdca52a981318775938de02f7efef88926ae1d7fd992988530",
					CeType:       "TestType",
					DecodedDBOps: jsonToDBOps(DB_OPS_2),
				},
			},
			false,
		},
		{
			"no-action-matching",
			"testdata/block-30080032.json",
			`{"issue":[{"key":"transaction_id", "type":"TestType"}]}`,
			nil,
			false,
		},
		{
			"action-filter-no-matching",
			"testdata/block-30080032.json",
			`{"create":[{"filter":"unknown","key":"transaction_id", "type":"TestType"}]}`,
			[]Generation{
				{
					Key:    "a2a53dce154c2ccdca52a981318775938de02f7efef88926ae1d7fd992988530",
					CeType: "TestType",
				},
			},
			false,
		},
		{
			"action-filter-no-matching-error",
			"testdata/block-30080032.json",
			`{"create":[{"filter":"unknown","key":"string(db_ops[0].new_json.id)", "type":"TestType"}]}`,
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(path.Base(tt.name), func(t *testing.T) {

			byteValue := readFileFromTestdata(t, tt.file)

			block := &pbcodec.Block{}
			// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
			err := json.Unmarshal(byteValue, block)
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}
			var localABIFiles = map[string]string{
				"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
			}
			abiFiles, err := LoadABIFiles(localABIFiles)
			if err != nil {
				t.Fatalf("LoadABIFiles() error: %v", err)
			}
			abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())
			actionsConfig := actionsConfFromJSON(t, tt.config)
			generator, err := NewActionsGenerator(actionsConfig)
			if err != nil {
				t.Fatalf("NewActionGenerator() error: %v", err)
			}

			trx := block.TransactionTraces()[0]
			act := trx.ActionTraces[0]
			decodedDBOps, err := abiDecoder.DecodeDBOps(trx.DBOpsForAction(act.ExecutionIndex), block.Number)
			if err != nil {
				t.Fatalf("DecodeDBOps() error: %v", err)
			}
			generations, err := generator.Apply("New", trx, act, decodedDBOps)
			if (err != nil) != tt.wantErr {
				t.Errorf("Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			} else if !reflect.DeepEqual(toJSON(generations), toJSON(tt.want)) {
				t.Errorf("\nApply() = \n%s\nWant: \n%s", toJSON(generations), toJSON(tt.want))
			}
		})
	}
}

var DB_OPS_2 string = `
[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	},
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 26,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": {"amount":0,"precision":8,"symbol":"USD"},
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	}
]
`

var DB_OPS_NEXT_FACTORY string = `
[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	}
]
`

var DB_OPS_FACTORY_A string = `
[
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 26,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": {"amount":0,"precision":8,"symbol":"USD"},
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	}
]
`

var DB_OPS_4 string = `
[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	},
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 26,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": {"amount":0,"precision":8,"symbol":"USD"},
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	},
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 28
		},
		"old_json": {
			"value": 27
		}
	},
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 27,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": {"amount":0,"precision":8,"symbol":"USD"},
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	}
]
`

var DB_OPS_4_FILTER string = `
[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	},
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 28
		},
		"old_json": {
			"value": 27
		}
	}
]
`

func Test_operation_on(t *testing.T) {
	type args struct {
		decodedDBOps string
	}
	tests := []struct {
		name string
		op   func() projection
		args args
		want string
	}{
		{
			name: "identity",
			op:   func() projection { return indentity{} },
			args: args{
				decodedDBOps: DB_OPS_2,
			},
			want: DB_OPS_2,
		},
		{
			name: "filter-first",
			op:   func() projection { return filter{tableNameMatcher{"next.factory"}} },
			args: args{
				decodedDBOps: DB_OPS_2,
			},
			want: DB_OPS_NEXT_FACTORY,
		},
		{
			name: "filter-last",
			op:   func() projection { return filter{tableNameMatcher{"factory.a"}} },
			args: args{
				decodedDBOps: DB_OPS_2,
			},
			want: DB_OPS_FACTORY_A,
		},
		{
			name: "filter-multi",
			op:   func() projection { return filter{tableNameMatcher{"next.factory"}} },
			args: args{
				decodedDBOps: DB_OPS_4,
			},
			want: DB_OPS_4_FILTER,
		},
		{
			name: "first-next.factory",
			op:   func() projection { return first{tableNameMatcher{"next.factory"}} },
			args: args{
				decodedDBOps: DB_OPS_4,
			},
			want: DB_OPS_NEXT_FACTORY,
		},
		{
			name: "first-factory-a",
			op:   func() projection { return first{tableNameMatcher{"factory.a"}} },
			args: args{
				decodedDBOps: DB_OPS_4,
			},
			want: DB_OPS_FACTORY_A,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := jsonToDBOps(tt.want)
			if got := tt.op().on(jsonToDBOps(tt.args.decodedDBOps)); !reflect.DeepEqual(toJSON(got), toJSON(want)) {
				t.Errorf("projection.on() = %+v\nwant = %+v", string(toJSON(got)), string(toJSON(want)))
			}
		})
	}
}

func jsonToDBOp(dbop string) (result *decodedDBOp) {
	err := json.Unmarshal(json.RawMessage(dbop), &result)
	if err != nil {
		panic(fmt.Sprintf("Unmarshal() error: %v, on: %s", err, dbop))
	}
	return
}

func jsonToDBOps(dbop string) (result []*decodedDBOp) {
	err := json.Unmarshal(json.RawMessage(dbop), &result)
	if err != nil {
		panic(fmt.Sprintf("Unmarshal() error: %v, on: %s", err, dbop))
	}
	return
}

var DB_OP string = `
{
	"operation": 2,
	"code": "eosio.nft.ft",
	"table_name": "next.factory",
	"primary_key": "next.factory",
	"old_payer": "eosio.nft.ft",
	"new_payer": "eosio.nft.ft",
	"old_data": "GgAAAAAAAAA=",
	"new_data": "GwAAAAAAAAA=",
	"new_json": {
		"value": 27
	},
	"old_json": {
		"value": 26
	}
}
`

func Benchmark_matcher_match(b *testing.B) {
	tests := []struct {
		name       string
		expression string
	}{
		{
			"tablename-match",
			"next.factory",
		},
		{
			"tablename-match-op*",
			"*:next.factory",
		},
		{
			"tablename-no-match",
			"next.vincent",
		},
		{
			"operation-match",
			"2:*",
		},
		{
			"operation-match-no",
			"0:*",
		},
		{
			"table-operation-match",
			"2:next.factory",
		},
		{
			"table-operation-no-match-op",
			"0:next.factory",
		},
		{
			"table-operation-no-match-table",
			"2:next.factories",
		},
	}
	dbop := jsonToDBOp(DB_OP)
	var value bool
	b.Run("wattermark", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			value = dbop.TableName == tests[0].expression
		}
	})

	for _, tt := range tests {
		matcher, err := expressionToMatcher(tt.expression)
		if err != nil {
			b.Fatalf("expressionToMatcher() error: %v, on: %s", err, tt.expression)
		}
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				value = matcher.match(dbop.DBOp)
			}
		})
	}
	b.Log(value)
}

func Test_expressionToMatcher(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       matcher
		wantErr    bool
	}{
		{
			"short-tablename",
			"vincent",
			tableNameMatcher{"vincent"},
			false,
		},
		{
			"tablename",
			"*:vincent",
			tableNameMatcher{"vincent"},
			false,
		},
		{
			"operation",
			"2:*",
			operationMatcher{2},
			false,
		},
		{
			"operation-out-range-upper",
			"4:*",
			nil,
			true,
		},
		{
			"operation-out-range-lower",
			"-1:*",
			nil,
			true,
		},
		{
			"operation-on-table",
			"2:vincent",
			operationOnTableMatcher{"vincent", 2},
			false,
		},
		{
			"unknown",
			"*:*",
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := expressionToMatcher(tt.expression)
			if (err != nil) != tt.wantErr {
				t.Errorf("expressionToMatcher() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("expressionToMatcher() = %v, want %v", got, tt.want)
			}
		})
	}
}
