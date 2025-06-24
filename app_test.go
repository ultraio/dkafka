package dkafka

import (
	"reflect"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

func Test_getCompressionLevel(t *testing.T) {
	type args struct {
		compressionType string
		config          *Config
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{"default", args{"snappy", &Config{KafkaCompressionLevel: -1}}, -1},
		{"gzip normal", args{"gzip", &Config{KafkaCompressionLevel: 2}}, 2},
		{"gzip outer up", args{"gzip", &Config{KafkaCompressionLevel: 12}}, 9},
		{"gzip outer down", args{"gzip", &Config{KafkaCompressionLevel: -2}}, 0},
		{"snappy normal", args{"snappy", &Config{KafkaCompressionLevel: 0}}, 0},
		{"snappy outer up", args{"snappy", &Config{KafkaCompressionLevel: 12}}, 0},
		{"snappy outer down", args{"snappy", &Config{KafkaCompressionLevel: -2}}, 0},
		{"lz4 normal", args{"lz4", &Config{KafkaCompressionLevel: 12}}, 12},
		{"lz4 outer up", args{"lz4", &Config{KafkaCompressionLevel: 143}}, 12},
		{"lz4 outer down", args{"lz4", &Config{KafkaCompressionLevel: -12}}, 0},
		{"zstd normal", args{"zstd", &Config{KafkaCompressionLevel: 2}}, -1},
		{"zstd outer up", args{"zstd", &Config{KafkaCompressionLevel: 12}}, -1},
		{"zstd outer down", args{"zstd", &Config{KafkaCompressionLevel: -2}}, -1},
		{"unknown", args{"????", &Config{KafkaCompressionLevel: 4}}, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getCompressionLevel(tt.args.compressionType, tt.args.config); got != tt.want {
				t.Errorf("getCompressionLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getCorrelation(t *testing.T) {
	type args struct {
		Actions []*pbcodec.ActionTrace
	}
	tests := []struct {
		Name            string
		Args            args
		WantCorrelation *Correlation
		WantErr         bool
	}{
		{"empty", args{}, nil, false},
		{"not-empty-no-correlations", args{[]*pbcodec.ActionTrace{{Action: &pbcodec.Action{Account: "eosio.token", Name: "transfer"}}}}, nil, false},
		{"not-empty-with-correlations-first", args{[]*pbcodec.ActionTrace{{Action: &pbcodec.Action{Account: "ultra.tools", Name: "correlate", JsonData: `{"payer":"ultra", "correlation_id":"123"}`}}, {Action: &pbcodec.Action{Account: "eosio.token", Name: "transfer"}}}}, &Correlation{"ultra", "123"}, false},
		{"missing-correlations-json-data", args{[]*pbcodec.ActionTrace{{Action: &pbcodec.Action{Account: "ultra.tools", Name: "correlate"}}}}, nil, true},
		{"not-empty-with-correlations-second", args{[]*pbcodec.ActionTrace{{Action: &pbcodec.Action{Account: "eosio.token", Name: "transfer"}}, {Action: &pbcodec.Action{Account: "ultra.tools", Name: "correlate", JsonData: `{"payer":"ultra", "correlation_id":"123"}`}}}}, &Correlation{"ultra", "123"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			gotCorrelation, err := getCorrelation(tt.Args.Actions)
			if (err != nil) != tt.WantErr {
				t.Errorf("getCorrelation() error = %v, wantErr %v", err, tt.WantErr)
				return
			}
			if !reflect.DeepEqual(gotCorrelation, tt.WantCorrelation) {
				t.Errorf("getCorrelation() = %v, want %v", gotCorrelation, tt.WantCorrelation)
			}
		})
	}
}

func Test_buildTableKeyExtractorFinder(t *testing.T) {
	dbOp := &pbcodec.DBOp{
		Scope:      "vince",
		PrimaryKey: "42",
	}
	type finderCheck struct {
		tableName string
		dbOp      *pbcodec.DBOp
		wantKey   string
	}
	tests := []struct {
		name    string
		args    []string
		want    []finderCheck
		wantErr bool
	}{
		{
			name:    "too many elements",
			args:    []string{"vincent:ana:mark"},
			wantErr: true,
		},
		{
			name:    "bad key mapping option",
			args:    []string{"account:?"},
			wantErr: true,
		},
		{
			name: "primary key extractor",
			args: []string{"account:k"},
			want: []finderCheck{{"account", dbOp, "42"}},
		},
		{
			name: "full key extractor",
			args: []string{"account:s+k"},
			want: []finderCheck{{"account", dbOp, "vince:42"}},
		},
		{
			name: "scope key extractor",
			args: []string{"account:s"},
			want: []finderCheck{{"account", dbOp, "vince"}},
		},
		{
			name: "default key extractor (primaryKey)",
			args: []string{"account"},
			want: []finderCheck{{"account", dbOp, "vince:42"}},
		},
		{
			name: "multi tables",
			args: []string{"account:k", "token:s+k"},
			want: []finderCheck{{"account", dbOp, "42"}, {"token", dbOp, "vince:42"}},
		},
		{
			name: "wildcard table filter with default key mapping",
			args: []string{"*"},
			want: []finderCheck{{"account", dbOp, "vince:42"}, {"token", dbOp, "vince:42"}},
		},
		{
			name: "wildcard table filter with specific primary key mapping",
			args: []string{"*:k"},
			want: []finderCheck{{"account", dbOp, "42"}, {"token", dbOp, "42"}},
		},
		{
			name: "wildcard table filter with specific full key mapping",
			args: []string{"*:s+k"},
			want: []finderCheck{{"account", dbOp, "vince:42"}, {"token", dbOp, "vince:42"}},
		},
		{
			name: "mix of wildcard table filter and specific name table filter",
			args: []string{"*:s+k", "account:k"},
			want: []finderCheck{{"account", dbOp, "42"}, {"token", dbOp, "vince:42"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFinder, err := buildTableKeyExtractorFinder(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildTableKeyExtractorFinder(tablesConfig) error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			for _, finderCheck := range tt.want {
				keyExtractorFunc, found := gotFinder(finderCheck.tableName)
				if !found {
					t.Errorf("TableKeyExtractorFinder(tableName) not found for table= %s", finderCheck.tableName)
				}
				if key := keyExtractorFunc(finderCheck.dbOp); key != finderCheck.wantKey {
					t.Errorf("(table=%s) ExtractKey(dbOp)= %s, want %s", finderCheck.tableName, key, finderCheck.wantKey)
				}
			}
		})
	}
}

func Test_createCdcKeyExpressions(t *testing.T) {
	tests := []struct {
		name          string
		cdcExpression string
		knownActions  []string
		unknownAction string
		wantErr       bool
	}{
		{
			name:          "invalid-expression",
			cdcExpression: "test",
			wantErr:       true,
		},
		{
			name:          "single",
			cdcExpression: "{\"create\": \"transaction_id\"}",
			knownActions:  []string{"create"},
			unknownAction: "issue",
		},
		{
			name:          "multi",
			cdcExpression: "{\"create\": \"transaction_id\", \"buy\": \"transaction_id\"}",
			knownActions:  []string{"create", "buy"},
			unknownAction: "issue",
		},
		{
			name:          "only-wildcard",
			cdcExpression: "{\"*\": \"first_auth_actor\"}",
			knownActions:  []string{"create", "buy"},
		},
		{
			name:          "action-and-wildcard",
			cdcExpression: "{\"create\": \"transaction_id\", \"*\": \"first_auth_actor\"}",
			knownActions:  []string{"create", "buy", "any"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFinder, err := createCdcKeyExpressions(tt.cdcExpression)
			if (err != nil) != tt.wantErr {
				t.Errorf("createCdcKeyExpressions() error = %v, wantErr %v", err, tt.wantErr)
				return
			} else if !tt.wantErr {
				for _, knownAction := range tt.knownActions {
					if _, found := gotFinder(knownAction); !found {
						t.Errorf("createCdcKeyExpressions(...)(%s) not found", knownAction)
						return
					}
				}
				if _, found := gotFinder(tt.unknownAction); tt.unknownAction != "" && found {
					t.Errorf("createCdcKeyExpressions(...)(%s) found while should be unknown", tt.unknownAction)
					return
				}
			}
		})
	}
}

func Test_addExecutedFilter(t *testing.T) {
	type args struct {
		filter   string
		executed bool
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "executed with filter",
			args: args{
				filter:   "filter",
				executed: true,
			},
			want: "executed && filter",
		},
		{
			name: "executed without filter",
			args: args{
				filter:   "",
				executed: true,
			},
			want: "executed",
		},
		{
			name: "not executed without filter",
			args: args{
				filter:   "",
				executed: false,
			},
			want: "",
		},
		{
			name: "not executed with filter",
			args: args{
				filter:   "filter",
				executed: false,
			},
			want: "filter",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := addExecutedFilter(tt.args.filter, tt.args.executed); got != tt.want {
				t.Errorf("addExecutedFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessageSchemaGenerator_namespace(t *testing.T) {
	type fields struct {
		Namespace    string
		MajorVersion uint
		Version      string
		Account      string
	}
	type args struct {
		kind    string
		account string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		// eosio.eba
		{
			name: "eosio.eba-actions",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "eosio.eba",
			},
			args: args{
				kind:    ACTIONS_CDC_TYPE,
				account: "eosio.eba",
			},
			want: "io.dkafka.data.eosio.eba.actions.v1",
		},
		{
			name: "eosio.eba-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "eosio.eba",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "eosio.eba",
			},
			want: "io.dkafka.data.eosio.eba.tables.v1",
		},
		// eosio.evm
		{
			name: "eosio.evm-actions",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "eosio.evm",
			},
			args: args{
				kind:    ACTIONS_CDC_TYPE,
				account: "eosio.evm",
			},
			want: "io.dkafka.data.eosio.evm.actions.v1",
		},
		{
			name: "eosio.evm-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "eosio.evm",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "eosio.evm",
			},
			want: "io.dkafka.data.eosio.evm.tables.v1",
		},
		// eosio.group
		{
			name: "eosio.group-actions",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "eosio.group",
			},
			args: args{
				kind:    ACTIONS_CDC_TYPE,
				account: "eosio.group",
			},
			want: "io.dkafka.data.eosio.group.actions.v1",
		},
		{
			name: "eosio.group-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "eosio.group",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "eosio.group",
			},
			want: "io.dkafka.data.eosio.group.tables.v1",
		},
		// eosio.nft.ft
		{
			name: "eosio.nft.ft-actions",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 2,
				Version:      "1.2.3",
				Account:      "eosio.nft.ft",
			},
			args: args{
				kind:    ACTIONS_CDC_TYPE,
				account: "eosio.nft.ft",
			},
			want: "io.dkafka.data.eosio.nft.ft.actions.v2",
		},
		{
			name: "eosio.nft.ft-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 2,
				Version:      "1.2.3",
				Account:      "eosio.nft.ft",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "eosio.nft.ft",
			},
			want: "io.dkafka.data.eosio.nft.ft.tables.v2",
		},
		// eosio.token
		{
			name: "eosio.token-actions",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 2,
				Version:      "1.2.3",
				Account:      "eosio.token",
			},
			args: args{
				kind:    ACTIONS_CDC_TYPE,
				account: "eosio.token",
			},
			want: "io.dkafka.data.eosio.token.actions.v2",
		},
		{
			name: "eosio.token-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 2,
				Version:      "1.2.3",
				Account:      "eosio.token",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "eosio.token",
			},
			want: "io.dkafka.data.eosio.token.tables.v2",
		},
		// ultra.claim
		{
			name: "ultra.claim-actions",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "ultra.claim",
			},
			args: args{
				kind:    ACTIONS_CDC_TYPE,
				account: "ultra.claim",
			},
			want: "io.dkafka.data.ultra.claim.actions.v1",
		},
		{
			name: "ultra.claim-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "ultra.claim",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "ultra.claim",
			},
			want: "io.dkafka.data.ultra.claim.tables.v1",
		},
		// ultra.rgrab
		{
			name: "ultra.rgrab-actions",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "ultra.rgrab",
			},
			args: args{
				kind:    ACTIONS_CDC_TYPE,
				account: "ultra.rgrab",
			},
			want: "io.dkafka.data.ultra.rgrab.actions.v1",
		},
		{
			name: "ultra.rgrab-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "ultra.rgrab",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "ultra.rgrab",
			},
			want: "io.dkafka.data.ultra.rgrab.tables.v1",
		},
		{
			name: "empty-namespace-actions",
			fields: fields{
				Namespace:    "",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "ultra.rgrab",
			},
			args: args{
				kind:    ACTIONS_CDC_TYPE,
				account: "ultra.rgrab",
			},
			want: "ultra.rgrab.actions.v1",
		},
		{
			name: "tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "ultra.rgrab",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "ultra.rgrab",
			},
			want: "io.dkafka.data.ultra.rgrab.tables.v1",
		},
		{
			name: "empty-namespace-tables",
			fields: fields{
				Namespace:    "",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "ultra.rgrab",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "ultra.rgrab",
			},
			want: "ultra.rgrab.tables.v1",
		},
		{
			name: "eba-account-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "aa1aa2aa3aa4",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "aa1aa2aa3aa4",
			},
			want: "io.dkafka.data.aa1aa2aa3aa4.tables.v1",
		},
		{
			name: "non-eba-account-tables",
			fields: fields{
				Namespace:    "io.dkafka.data",
				MajorVersion: 1,
				Version:      "1.2.3",
				Account:      "1aa2aa3aa4bx",
			},
			args: args{
				kind:    TABLES_CDC_TYPE,
				account: "1aa2aa3aa4bx",
			},
			want: "io.dkafka.data._1aa2aa3aa4bx.tables.v1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := MessageSchemaGenerator{
				Namespace:    tt.fields.Namespace,
				MajorVersion: tt.fields.MajorVersion,
				Version:      tt.fields.Version,
				Account:      tt.fields.Account,
			}
			if got := msg.namespace(tt.args.kind, tt.args.account); got != tt.want {
				t.Errorf("MessageSchemaGenerator.namespace() = %v, want %v", got, tt.want)
			}
		})
	}
}
