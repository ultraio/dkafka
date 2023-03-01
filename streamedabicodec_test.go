package dkafka

import (
	"reflect"
	"testing"

	"github.com/eoscanada/eos-go"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
)

func TestStreamedABICodec_doUpdateABI(t *testing.T) {
	type args struct {
		abi      *eos.ABI
		blockNum uint32
		step     pbbstream.ForkStep
	}
	tests := []struct {
		name string
		sut  *StreamedAbiCodec
		args args
		want *StreamedAbiCodec
	}{
		{
			name: "empty-irreversible",
			sut:  &StreamedAbiCodec{},
			args: args{
				abi:      &eos.ABI{Version: "123"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     42,
					irreversible: true,
				},
			},
		},
		{
			name: "empty-new",
			sut:  &StreamedAbiCodec{},
			args: args{
				abi:      &eos.ABI{Version: "123"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_NEW,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     42,
					irreversible: false,
				},
			},
		},
		{
			name: "not-empty-irreversible",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: false,
				},
				abiHistory: []*AbiItem{{abi: &eos.ABI{Version: "123"},
					blockNum:     1,
					irreversible: true,
				}},
			},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     42,
						irreversible: false,
					},
				},
			},
		},
		{
			name: "irreversible-compaction",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     42,
						irreversible: false,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "789"},
				blockNum: 64,
				step:     pbbstream.ForkStep_STEP_NEW,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "789"},
					blockNum:     64,
					irreversible: false,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     42,
						irreversible: true,
					},
				},
			},
		},
		{
			name: "irreversible-only",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "789"},
				blockNum: 64,
				step:     pbbstream.ForkStep_STEP_IRREVERSIBLE,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "789"},
					blockNum:     64,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     42,
						irreversible: true,
					},
				},
			},
		},
		{
			name: "undo",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: false,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     1,
					irreversible: true,
				},
			},
		},
		{
			name: "unknown",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: false,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNKNOWN,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     42,
					irreversible: false,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
		},
		{
			name: "undo-empty",
			sut:  &StreamedAbiCodec{},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{},
		},
		{
			name: "undo-gt-latest",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     24,
					irreversible: true,
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "456"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "123"},
					blockNum:     24,
					irreversible: true,
				},
			},
		},
		{
			name: "undo-long-history",
			sut: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "789"},
					blockNum:     42,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
					{
						abi:          &eos.ABI{Version: "456"},
						blockNum:     24,
						irreversible: true,
					},
				},
			},
			args: args{
				abi:      &eos.ABI{Version: "789"},
				blockNum: 42,
				step:     pbbstream.ForkStep_STEP_UNDO,
			},
			want: &StreamedAbiCodec{
				latestABI: &AbiItem{
					abi:          &eos.ABI{Version: "456"},
					blockNum:     24,
					irreversible: true,
				},
				abiHistory: []*AbiItem{
					{
						abi:          &eos.ABI{Version: "123"},
						blockNum:     1,
						irreversible: true,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.sut.doUpdateABI(tt.args.abi, tt.args.blockNum, tt.args.step)
			assertEqual(t, tt.sut, tt.want)
		})
	}
}

func assertEqual(t *testing.T, actual, expected *StreamedAbiCodec) {
	if !reflect.DeepEqual(actual, expected) {
		equal := assertFieldsEqual(t, "abiHistory", actual.abiHistory, expected.abiHistory) &&
			assertFieldsEqual(t, "latestABI.abi", actual.latestABI.abi, expected.latestABI.abi) &&
			assertFieldsEqual(t, "latestABI.blockNum", actual.latestABI.blockNum, expected.latestABI.blockNum) &&
			assertFieldsEqual(t, "latestABI.irreversible", actual.latestABI.irreversible, expected.latestABI.irreversible)
		if equal {
			// the test above did detect the diff use default solution
			t.Errorf("doUpdateABI() = %v, want %v", actual, expected)
		}
	}
}

func assertFieldsEqual(t *testing.T, path string, actual, expected interface{}) bool {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("%s:\nactual = %v\nwant   = %v", path, actual, expected)
		return false
	}
	return true
}
