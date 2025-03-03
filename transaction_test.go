package dkafka

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/golang/protobuf/jsonpb"
	"github.com/riferrei/srclient"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Test_transactionGenerator_Apply(t *testing.T) {
	timestamp := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	blockStep := BlockStep{
		blk: &pbcodec.Block{
			Header: &pbcodec.BlockHeader{
				Timestamp: timestamppb.New(timestamp),
			},
		},
		step:   pbbstream.ForkStep_STEP_NEW,
		cursor: "123",
	}

	type fields struct {
		headers []kafka.Header
		topic   string
	}

	type expectedValues struct {
		key   string
		ceId  string
		value interface{}
	}

	tests := []struct {
		name    string
		fields  fields
		args    TransactionContext
		expect  expectedValues
		wantErr bool
	}{
		{
			name:   "default",
			fields: fields{topic: "dkafka.test", headers: default_headers},
			args: TransactionContext{
				stepName: "",
				transaction: &pbcodec.TransactionTrace{
					Id:              "trx-1",
					BlockNum:        0,
					Index:           0,
					BlockTime:       timestamppb.New(timestamp),
					ProducerBlockId: "",
					Receipt: &pbcodec.TransactionReceiptHeader{
						Status:               0,
						CpuUsageMicroSeconds: 0,
						NetUsageWords:        0,
					},
					Elapsed:   0,
					NetUsage:  0,
					Scheduled: false,
					ActionTraces: []*pbcodec.ActionTrace{{
						Receiver: "",
						Action: &pbcodec.Action{
							Account: "",
							Name:    "",
							Authorization: []*pbcodec.PermissionLevel{{
								Actor:      "",
								Permission: "",
							}},
						},
						ContextFree:     false,
						Elapsed:         0,
						Console:         "",
						TransactionId:   "",
						BlockNum:        0,
						ProducerBlockId: "",
						BlockTime:       timestamppb.New(timestamp),
						AccountRamDeltas: []*pbcodec.AccountRAMDelta{{
							Account: "",
							Delta:   0,
						}},
						Exception: &pbcodec.Exception{
							Code:    0,
							Name:    "",
							Message: "",
						},
						ErrorCode:                              0,
						ActionOrdinal:                          0,
						CreatorActionOrdinal:                   0,
						ClosestUnnotifiedAncestorActionOrdinal: 0,
						ExecutionIndex:                         0,
					}},
					FailedDtrxTrace: nil,
					Exception: &pbcodec.Exception{
						Code:    0,
						Name:    "",
						Message: "",
					},
					ErrorCode:  0,
					DbOps:      []*pbcodec.DBOp{},
					DtrxOps:    []*pbcodec.DTrxOp{},
					FeatureOps: []*pbcodec.FeatureOp{},
					PermOps:    []*pbcodec.PermOp{},
					RamOps: []*pbcodec.RAMOp{{
						Operation:   0,
						ActionIndex: 0,
						Payer:       "",
						Delta:       0,
						Usage:       0,
					}},
					RamCorrectionOps: []*pbcodec.RAMCorrectionOp{},
					RlimitOps:        []*pbcodec.RlimitOp{},
					TableOps:         []*pbcodec.TableOp{},
					CreationTree: []*pbcodec.CreationFlatNode{{
						CreatorActionIndex:   0,
						ExecutionActionIndex: 0,
					}},
				},
				blockStep: blockStep,
				cursor:    "",
				step:      0,
			},
			expect: expectedValues{
				key:  "trx-1",
				ceId: "trx-1",
				value: map[string]interface{}{
					"id":                map[string]interface{}{"string": "trx-1"},
					"block_num":         map[string]interface{}{"long": int64(0)},
					"index":             map[string]interface{}{"long": int64(0)},
					"block_time":        map[string]interface{}{"long.timestamp-millis": timestamp},
					"producer_block_id": map[string]interface{}{"string": ""},
					"receipt": map[string]interface{}{
						"io.ultra.dkafka.transaction.TransactionReceiptHeader": map[string]interface{}{
							"cpu_usage_micro_seconds": map[string]interface{}{"long": int64(0)},
							"status":                  map[string]interface{}{"int": int32(0)},
						}},
					"elapsed":   map[string]interface{}{"long": int64(0)},
					"net_usage": map[string]interface{}{"long": int64(0)},
					"action_traces": []interface{}{
						map[string]interface{}{
							"receiver": map[string]interface{}{"string": ""},
							"action": map[string]interface{}{"io.ultra.dkafka.transaction.Action": map[string]interface{}{
								"account": map[string]interface{}{"string": ""},
								"name":    map[string]interface{}{"string": ""},
								"authorization": []interface{}{
									map[string]interface{}{
										"actor":      map[string]interface{}{"string": ""},
										"permission": map[string]interface{}{"string": ""},
									},
								},
							}},
							"context_free": map[string]interface{}{"boolean": false},
							"elapsed":      map[string]interface{}{"long": int64(0)},
							"account_ram_deltas": []interface{}{
								map[string]interface{}{
									"account": map[string]interface{}{"string": ""},
									"delta":   map[string]interface{}{"long": int64(0)},
								},
							},
							"exception": map[string]interface{}{
								"io.ultra.dkafka.transaction.Exception": map[string]interface{}{
									"code":    map[string]interface{}{"int": int32(0)},
									"message": map[string]interface{}{"string": ""},
									"name":    map[string]interface{}{"string": ""},
								}},
							"error_code":             map[string]interface{}{"bytes.decimal": new(big.Rat).SetInt64(0)},
							"action_ordinal":         map[string]interface{}{"int": int32(0)},
							"creator_action_ordinal": map[string]interface{}{"int": int32(0)},
							"closest_unnotified_ancestor_action_ordinal": map[string]interface{}{"int": int32(0)},
							"execution_index": map[string]interface{}{"int": int32(0)},
						},
					},
					"exception": map[string]interface{}{
						"io.ultra.dkafka.transaction.Exception": map[string]interface{}{
							"code":    map[string]interface{}{"int": int32(0)},
							"message": map[string]interface{}{"string": ""},
							"name":    map[string]interface{}{"string": ""},
						}},
					"error_code": map[string]interface{}{"bytes.decimal": new(big.Rat).SetInt64(0)},
					"ram_ops": []interface{}{
						map[string]interface{}{
							"operation":    map[string]interface{}{"int": int32(0)},
							"action_index": map[string]interface{}{"long": int64(0)},
							"payer":        map[string]interface{}{"string": ""},
							"delta":        map[string]interface{}{"long": int64(0)},
							"usage":        map[string]interface{}{"long": int64(0)},
						},
					},
					"creation_tree": []interface{}{
						map[string]interface{}{
							"creator_action_index":   map[string]interface{}{"long": int64(0)},
							"execution_action_index": map[string]interface{}{"long": int64(0)},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:   "empty_arrays_object",
			fields: fields{topic: "dkafka.test", headers: default_headers},
			args: TransactionContext{
				stepName: "",
				transaction: &pbcodec.TransactionTrace{
					Id:               "trx-1",
					BlockNum:         0,
					Index:            0,
					BlockTime:        timestamppb.New(timestamp),
					ProducerBlockId:  "",
					Receipt:          nil,
					Elapsed:          0,
					NetUsage:         0,
					Scheduled:        false,
					ActionTraces:     []*pbcodec.ActionTrace{},
					FailedDtrxTrace:  nil,
					Exception:        nil,
					ErrorCode:        0,
					DbOps:            []*pbcodec.DBOp{},
					DtrxOps:          []*pbcodec.DTrxOp{},
					FeatureOps:       []*pbcodec.FeatureOp{},
					PermOps:          []*pbcodec.PermOp{},
					RamOps:           []*pbcodec.RAMOp{},
					RamCorrectionOps: []*pbcodec.RAMCorrectionOp{},
					RlimitOps:        []*pbcodec.RlimitOp{},
					TableOps:         []*pbcodec.TableOp{},
					CreationTree:     []*pbcodec.CreationFlatNode{},
				},
				blockStep: blockStep,
				cursor:    "",
				step:      0,
			},
			expect: expectedValues{
				key:  "trx-1",
				ceId: "trx-1",
				value: map[string]interface{}{
					"id":                map[string]interface{}{"string": "trx-1"},
					"block_num":         map[string]interface{}{"long": int64(0)},
					"index":             map[string]interface{}{"long": int64(0)},
					"block_time":        map[string]interface{}{"long.timestamp-millis": timestamp},
					"producer_block_id": map[string]interface{}{"string": ""},
					"receipt": map[string]interface{}{
						"io.ultra.dkafka.transaction.TransactionReceiptHeader": map[string]interface{}{
							"cpu_usage_micro_seconds": nil,
							"status":                  nil,
						}},
					"elapsed":       map[string]interface{}{"long": int64(0)},
					"net_usage":     map[string]interface{}{"long": int64(0)},
					"action_traces": []interface{}{},
					"exception": map[string]interface{}{
						"io.ultra.dkafka.transaction.Exception": map[string]interface{}{
							"code":    nil,
							"message": nil,
							"name":    nil,
						}},
					"error_code":    map[string]interface{}{"bytes.decimal": new(big.Rat).SetInt64(0)},
					"ram_ops":       []interface{}{},
					"creation_tree": []interface{}{},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			abiCodec := NewStreamedAbiCodecWithTransaction(&DfuseAbiRepository{},
				nil, srclient.CreateMockSchemaRegistryClient("mock://bench-adapter"), "", "mock://bench-adapter")
			codec, err := abiCodec.GetCodec(transactionSchema.AsCodecId(), 0)
			if err != nil {
				t.Fatalf("cannot load codec for %s", transactionNotification)
			}
			tgen := transactionGenerator{
				headers:  default_headers,
				topic:    tt.fields.topic,
				abiCodec: abiCodec,
			}
			messages, err := tgen.Apply(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(messages) > 1 {
				t.Errorf("Apply() got = %v, want a list of message of len: 1", messages)
			}
			// test the value
			gotValue, err := codec.Unmarshal(messages[0].Value)
			if err != nil {
				t.Errorf("Unmarshal() error = %v", err)
				return
			}
			expect := tt.expect.value
			if expect == nil {
				expect = tt.args.transaction
			}
			if !reflect.DeepEqual(gotValue, expect) {
				t.Errorf("codec Marshal() then Unmarshal() = %v, want %v", gotValue, expect)
			}
			// test the key
			if string(messages[0].Key) != tt.expect.key {
				t1.Errorf("Apply() got = %v, want %v", messages[0].Key, tt.expect.key)
			}

		})
	}
}

func Test_transactionGenerator_Apply_approval(t *testing.T) {
	tests := []struct {
		name       string
		file       string
		nbMessages int
	}{
		{
			"transaction-1",
			"testdata/block-transactions-6723651.pb.json",
			2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := &pbcodec.Block{}
			err := jsonpb.UnmarshalString(string(readFileFromTestdata(t, tt.file)), block)
			if err != nil {
				t.Fatalf("jsonpb.UnmarshalString(): %v", err)
			}

			abiCodec := NewStreamedAbiCodecWithTransaction(&DfuseAbiRepository{},
				nil, srclient.CreateMockSchemaRegistryClient("mock://bench-adapter"), "", "mock://bench-adapter")

			if err != nil {
				t.Fatalf("cannot load codec for %s", transactionNotification)
			}
			transactionGen := transactionGenerator{
				headers:  default_headers,
				topic:    "test.topic",
				abiCodec: abiCodec,
			}
			for i, trx := range block.TransactionTraces() {
				_, err := transactionGen.Apply(TransactionContext{
					block:       block,
					stepName:    "NEW",
					transaction: trx,
					blockStep: BlockStep{
						blk:            block,
						step:           pbbstream.ForkStep_STEP_NEW,
						cursor:         "???",
						previousCursor: "???",
					},
				})
				if err != nil {
					t.Errorf("transactionGen.Apply() at %d with error = %v", i, err)
					return
				}
			}
		})
	}
}
