package dkafka

import (
	"fmt"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

const emptyName = "............."

func Test_normalizeName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "non-empty",
			args: args{
				name: "eosio",
			},
			want: "eosio",
		},
		{
			name: "empty",
			args: args{
				name: "",
			},
			want: emptyName,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeName(tt.args.name); got != tt.want {
				t.Errorf("normalizeName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractFullKey(t *testing.T) {
	type args struct {
		dbOp *pbcodec.DBOp
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "non-empty",
			args: args{
				dbOp: &pbcodec.DBOp{
					Scope:      "eosio",
					PrimaryKey: "global",
				},
			},
			want: "eosio:global",
		},
		{
			name: "empty-scope",
			args: args{
				dbOp: &pbcodec.DBOp{
					PrimaryKey: "global",
				},
			},
			want: fmt.Sprintf("%s:global", emptyName),
		},
		{
			name: "empty-primary-key",
			args: args{
				dbOp: &pbcodec.DBOp{
					Scope: "eosio",
				},
			},
			want: fmt.Sprintf("eosio:%s", emptyName),
		},
		{
			name: "empty-scope-and-primary-key",
			args: args{
				dbOp: &pbcodec.DBOp{},
			},
			want: fmt.Sprintf("%s:%s", emptyName, emptyName),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractFullKey(tt.args.dbOp); got != tt.want {
				t.Errorf("extractFullKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractScope(t *testing.T) {
	type args struct {
		dbOp *pbcodec.DBOp
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "with-scope",
			args: args{
				dbOp: &pbcodec.DBOp{
					Scope: "eosio",
				},
			},
			want: "eosio",
		},
		{
			name: "without-scope",
			args: args{
				dbOp: &pbcodec.DBOp{},
			},
			want: emptyName,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractScope(tt.args.dbOp); got != tt.want {
				t.Errorf("extractScope() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractPrimaryKey(t *testing.T) {
	type args struct {
		dbOp *pbcodec.DBOp
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "with-primary-key",
			args: args{
				dbOp: &pbcodec.DBOp{
					PrimaryKey: "global",
				},
			},
			want: "global",
		},
		{
			name: "without-primary-key",
			args: args{
				dbOp: &pbcodec.DBOp{},
			},
			want: emptyName,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractPrimaryKey(tt.args.dbOp); got != tt.want {
				t.Errorf("extractPrimaryKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
