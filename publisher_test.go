package dkafka

import (
	"reflect"
	"testing"
)

func Test_newDBOpInfoRecord(t *testing.T) {
	type args struct {
		tableName string
		jsonData  RecordSchema
	}
	tests := []struct {
		name string
		args args
		want RecordSchema
	}{
		{
			name: "default",
			args: args{
				tableName: "TableA",
				jsonData: newRecordS(
					"Toto",
					[]FieldSchema{
						NewOptionalField("tata", "string"),
					},
				),
			},
			want: newRecordS(
				"TableA",
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
					NewOptionalField("old_json", newRecordS(
						"Toto",
						[]FieldSchema{
							NewOptionalField("tata", "string"),
						},
					)),
					NewOptionalField("new_json", "Toto"),
				},
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newDBOpInfoRecord(tt.args.tableName, tt.args.jsonData); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newDBOpInfoRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}
