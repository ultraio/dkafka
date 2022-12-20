package dkafka

import (
	"reflect"
	"testing"

	"github.com/eoscanada/eos-go"
)

func Test_resolveFieldTypeSchema(t *testing.T) {
	type args struct {
		fieldType string
		abi       *ABI
	}
	tests := []struct {
		name    string
		args    args
		want    Schema
		wantErr bool
	}{
		{
			name:    "bool->boolean",
			args:    args{"bool", nil},
			want:    "boolean",
			wantErr: false,
		},
		{
			name:    "int8->int",
			args:    args{"int8", nil},
			want:    "int",
			wantErr: false,
		},
		{
			name:    "uint8->int",
			args:    args{"uint8", nil},
			want:    "int",
			wantErr: false,
		},
		{
			name:    "int16->int",
			args:    args{"int16", nil},
			want:    "int",
			wantErr: false,
		},
		{
			name:    "uint16->int",
			args:    args{"uint16", nil},
			want:    "int",
			wantErr: false,
		},
		{
			name:    "int32->int",
			args:    args{"int32", nil},
			want:    "int",
			wantErr: false,
		},
		{
			name:    "int32[]->[]int",
			args:    args{"int32[]", nil},
			want:    NewArray("int"),
			wantErr: false,
		},
		{
			name:    "int32$->['null', 'int']",
			args:    args{"int32$", nil},
			want:    NewOptional("int"),
			wantErr: false,
		},
		{
			name:    "int32?$->['null','int']",
			args:    args{"int32?$", nil},
			want:    NewOptional("int"),
			wantErr: false,
		},
		{
			name:    "int32[]?->['null',[]int]",
			args:    args{"int32[]?$", nil},
			want:    NewOptional(NewArray("int")),
			wantErr: false,
		},
		{
			name:    "int32?->['null','int']",
			args:    args{"int32?", nil},
			want:    NewOptional("int"),
			wantErr: false,
		},
		{
			name:    "uint32->long",
			args:    args{"uint32", nil},
			want:    "long",
			wantErr: false,
		},
		{
			name:    "int64->long",
			args:    args{"int64", nil},
			want:    "long",
			wantErr: false,
		},
		{
			name:    "uint64->long",
			args:    args{"uint64", nil},
			want:    "long",
			wantErr: false,
		},
		{
			name:    "varint32->int",
			args:    args{"varint32", nil},
			want:    "int",
			wantErr: false,
		},
		{
			name:    "varuint32->long",
			args:    args{"varuint32", nil},
			want:    "long",
			wantErr: false,
		},
		{
			name:    "unknown->error",
			args:    args{"unknown", &ABI{&eos.ABI{}, 0}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "struct->record",
			args: args{"my_struct", &ABI{&eos.ABI{
				Types: []eos.ABIType{{
					NewTypeName: "int64_alias",
					Type:        "int64",
				}},
				Structs: []eos.StructDef{{
					Name: "my_struct",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "fieldA",
							Type: "uint32",
						},
						{
							Name: "fieldB",
							Type: "int64_alias",
						},
					},
				}},
			}, 42}},
			want: RecordSchema{
				Type: "record",
				Name: "MyStruct",
				Fields: []FieldSchema{
					{
						Name: "fieldA",
						Type: "long",
					},
					{
						Name: "fieldB",
						Type: "long",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "struct-inheritance",
			args: args{"my_struct", &ABI{&eos.ABI{
				Types: []eos.ABIType{{
					NewTypeName: "int64_alias",
					Type:        "int64",
				}},
				Structs: []eos.StructDef{
					{
						Name: "parent",
						Base: "",
						Fields: []eos.FieldDef{
							{
								Name: "parentfieldA",
								Type: "string",
							},
							{
								Name: "parentFieldB",
								Type: "int32",
							},
						},
					},
					{
						Name: "my_struct",
						Base: "parent",
						Fields: []eos.FieldDef{
							{
								Name: "fieldA",
								Type: "uint32",
							},
							{
								Name: "fieldB",
								Type: "int64_alias",
							},
						},
					}},
			}, 42}},
			want: RecordSchema{
				Type: "record",
				Name: "MyStruct",
				Fields: []FieldSchema{
					{
						Name: "parentfieldA",
						Type: "string",
					},
					{
						Name: "parentFieldB",
						Type: "int",
					},
					{
						Name: "fieldA",
						Type: "long",
					},
					{
						Name: "fieldB",
						Type: "long",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveFieldTypeSchema(tt.args.abi, tt.args.fieldType, make(map[string]string))
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveFieldTypeSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resolveFieldTypeSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

var actionABI eos.ABI = eos.ABI{
	Types: []eos.ABIType{{
		NewTypeName: "int64_alias",
		Type:        "int64",
	}},
	Structs: []eos.StructDef{{
		Name: "my_action",
		Base: "",
		Fields: []eos.FieldDef{
			{
				Name: "fieldA",
				Type: "uint32",
			},
			{
				Name: "fieldB",
				Type: "int64_alias",
			},
		},
	}},
	Actions: []eos.ActionDef{{
		Name: "my_action",
		Type: "my_action",
	}},
}

func TestActionToRecord(t *testing.T) {
	type args struct {
		abi  *ABI
		name eos.ActionName
	}
	tests := []struct {
		name    string
		args    args
		want    RecordSchema
		wantErr bool
	}{
		{
			"known_action",
			args{
				&ABI{&actionABI, 42},
				"my_action",
			},
			RecordSchema{
				Type: "record",
				Name: "MyAction",
				Fields: []FieldSchema{
					{
						Name: "fieldA",
						Type: "long",
					},
					{
						Name: "fieldB",
						Type: "long",
					},
				},
			},
			false,
		},
		{
			"unknown_action",
			args{
				&ABI{&actionABI, 42},
				"unknown_action",
			},
			RecordSchema{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ActionToRecord(tt.args.abi, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("ActionToRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ActionToRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}

var tableABI eos.ABI = eos.ABI{
	Types: []eos.ABIType{{
		NewTypeName: "int64_alias",
		Type:        "int64",
	}},
	Structs: []eos.StructDef{{
		Name: "my_table_struct",
		Base: "",
		Fields: []eos.FieldDef{
			{
				Name: "fieldA",
				Type: "uint32",
			},
			{
				Name: "fieldB",
				Type: "int64_alias",
			},
		},
	}},
	Tables: []eos.TableDef{{
		Name:      "my.table",
		IndexType: "i64",
		Type:      "my_table_struct",
	}},
}

func TestTableToRecord(t *testing.T) {
	type args struct {
		abi  *ABI
		name eos.TableName
	}
	tests := []struct {
		name    string
		args    args
		want    RecordSchema
		wantErr bool
	}{
		{
			"known table",
			args{
				&ABI{&tableABI, 42},
				"my.table",
			},
			RecordSchema{
				Type: "record",
				Name: "MyTableStruct",
				Fields: []FieldSchema{
					{
						Name: "fieldA",
						Type: "long",
					},
					{
						Name: "fieldB",
						Type: "long",
					},
				},
			},
			false,
		},
		{
			"unknown table",
			args{
				&ABI{&actionABI, 42},
				"unknown.table",
			},
			RecordSchema{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TableToRecord(tt.args.abi, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("TableToRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TableToRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}
