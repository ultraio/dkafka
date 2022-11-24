package dkafka

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

func Test_exprToCelProgram(t *testing.T) {
	byteValue := readFileFromTestdata(t, "testdata/block-30080032.json")

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

	transactionTrace := block.TransactionTraces()[0]
	act := transactionTrace.ActionTraces[0]
	decodedDBOps, err := abiDecoder.DecodeDBOps(transactionTrace.DBOpsForAction(act.ExecutionIndex), block.Number)
	if err != nil {
		t.Fatalf("DecodeDBOps() error: %v", err)
	}
	activation, err := NewActivation("NEW", transactionTrace, act, decodedDBOps)
	if err != nil {
		t.Fatalf("NewActivation() error: %v", err)
	}

	type args struct {
		expression string
	}
	tests := []struct {
		name       string
		args       args
		wantString string
		wantErr    bool
	}{
		{
			"block_num",
			args{
				"string(block_num)",
			},
			"30080032",
			false,
		},
		{
			"block_num",
			args{
				"string(block_num)",
			},
			"30080032",
			false,
		},
		{
			"block_id",
			args{
				"block_id",
			},
			"01cafc203bf4bf807266fe5ac12c4b9ef72bd6482367a6c95bff70161dbeb462",
			false,
		},
		{
			"block_time",
			args{
				"block_time",
			},
			"2021-11-17T10:16:19Z",
			false,
		},
		{
			"transaction_id",
			args{
				"transaction_id",
			},
			"a2a53dce154c2ccdca52a981318775938de02f7efef88926ae1d7fd992988530",
			false,
		},
		{
			"step",
			args{
				"step",
			},
			"NEW",
			false,
		},
		{
			"global_seq",
			args{
				"string(global_seq)",
			},
			"80493723",
			false,
		},
		{
			"execution_index",
			args{
				"string(execution_index)",
			},
			"0",
			false,
		},
		{
			"receiver",
			args{
				"receiver",
			},
			"eosio.nft.ft",
			false,
		},
		{
			"account",
			args{
				"account",
			},
			"eosio.nft.ft",
			false,
		},
		{
			"action",
			args{
				"action",
			},
			"create",
			false,
		},
		{
			"auth",
			args{
				"auth[0]",
			},
			"ultra.nft.ft",
			false,
		},
		{
			"data",
			args{
				"data.create.asset_creator",
			},
			"ultra.nft.ft",
			false,
		},
		{
			"db_ops",
			args{
				"db_ops[0].table_name",
			},
			"next.factory",
			false,
		},
		{
			"db_ops",
			args{
				"string(db_ops[0].old_json.value)",
			},
			"26",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProg, err := exprToCelProgram(tt.args.expression)
			if (err != nil) != tt.wantErr {
				t.Errorf("exprToCelProgram() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotString, err := evalString(gotProg, activation)
			if (err != nil) != tt.wantErr {
				t.Errorf("evalString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotString, tt.wantString) {
				t.Errorf("evalString() = %v, want %v", gotString, tt.wantString)
			}
		})
	}
}

func Benchmark_exprToCelProgram_activation(b *testing.B) {
	byteValue := readFileFromTestdata(b, "testdata/block-30080032.json")

	block := &pbcodec.Block{}
	// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
	err := json.Unmarshal(byteValue, block)
	if err != nil {
		b.Fatalf("Unmarshal() error: %v", err)
	}
	var localABIFiles = map[string]string{
		"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
	}
	abiFiles, err := LoadABIFiles(localABIFiles)
	if err != nil {
		b.Fatalf("LoadABIFiles() error: %v", err)
	}
	abiDecoder := NewABIDecoder(abiFiles, nil, context.Background())

	transactionTrace := block.TransactionTraces()[0]
	act := transactionTrace.ActionTraces[0]
	decodedDBOps, err := abiDecoder.DecodeDBOps(transactionTrace.DBOpsForAction(act.ExecutionIndex), block.Number)
	if err != nil {
		b.Fatalf("DecodeDBOps() error: %v", err)
	}
	activation, err := NewActivation("New", transactionTrace, act, decodedDBOps)
	if err != nil {
		b.Fatalf("NewActivation() error: %v", err)
	}

	type args struct {
		expression string
	}
	tests := []struct {
		name       string
		args       args
		wantString string
		wantErr    bool
	}{
		{
			"block_num",
			args{
				"string(block_num)",
			},
			"30080032",
			false,
		},
		{
			"block_num",
			args{
				"string(block_num)",
			},
			"30080032",
			false,
		},
		{
			"block_id",
			args{
				"block_id",
			},
			"01cafc203bf4bf807266fe5ac12c4b9ef72bd6482367a6c95bff70161dbeb462",
			false,
		},
		{
			"block_time",
			args{
				"block_time",
			},
			"2021-11-17T10:16:19Z",
			false,
		},
		{
			"transaction_id",
			args{
				"transaction_id",
			},
			"a2a53dce154c2ccdca52a981318775938de02f7efef88926ae1d7fd992988530",
			false,
		},
		{
			"step",
			args{
				"step",
			},
			"New",
			false,
		},
		{
			"global_seq",
			args{
				"string(global_seq)",
			},
			"80493723",
			false,
		},
		{
			"execution_index",
			args{
				"string(execution_index)",
			},
			"0",
			false,
		},
		{
			"receiver",
			args{
				"receiver",
			},
			"eosio.nft.ft",
			false,
		},
		{
			"account",
			args{
				"account",
			},
			"eosio.nft.ft",
			false,
		},
		{
			"action",
			args{
				"action",
			},
			"create",
			false,
		},
		{
			"auth",
			args{
				"auth[0]",
			},
			"ultra.nft.ft",
			false,
		},
		{
			"data",
			args{
				"data.create.asset_creator",
			},
			"ultra.nft.ft",
			false,
		},
		{
			"db_ops",
			args{
				"db_ops[0].table_name",
			},
			"next.factory",
			false,
		},
	}
	for _, tt := range tests {
		gotProg, err := exprToCelProgram(tt.args.expression)
		if (err != nil) != tt.wantErr {
			b.Errorf("exprToCelProgram() error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := evalString(gotProg, activation)
				if err != nil {
					b.Errorf("evalString() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}
		})
	}
}
