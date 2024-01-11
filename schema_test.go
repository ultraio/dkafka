package dkafka

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/eoscanada/eos-go"
)

func Test_resolveFieldTypeSchema(t *testing.T) {
	initBuiltInTypesForTables()
	type args struct {
		fieldType string
		abi       *ABI
	}
	type test struct {
		name    string
		args    args
		want    Schema
		wantErr bool
	}

	newTest := func(eosType string, want string, wantErr bool) test {
		return test{
			name:    fmt.Sprintf("%s->%s", eosType, want),
			args:    args{eosType, nil},
			want:    map[string]interface{}{"type": want, "eos.type": eosType},
			wantErr: wantErr,
		}
	}

	tests := []test{
		newTest(
			"bool",
			"boolean",
			false),
		newTest(
			"int8",
			"int",
			false,
		),
		newTest(
			"uint8",
			"int",
			false,
		),
		newTest(
			"int16",
			"int",
			false,
		),
		newTest(
			"uint16",
			"int",
			false,
		),
		newTest(
			"int32",
			"int",
			false,
		),
		{
			name:    "int32[]->[]int",
			args:    args{"int32[]", nil},
			want:    NewArray(map[string]interface{}{"type": "int", "eos.type": "int32"}),
			wantErr: false,
		},
		{
			name:    "int32$->['null', 'int']",
			args:    args{"int32$", nil},
			want:    NewOptional(map[string]interface{}{"type": "int", "eos.type": "int32"}),
			wantErr: false,
		},
		{
			name:    "int32?$->['null','int']",
			args:    args{"int32?$", nil},
			want:    NewOptional(map[string]interface{}{"type": "int", "eos.type": "int32"}),
			wantErr: false,
		},
		{
			name:    "int32[]?->['null',[]int]",
			args:    args{"int32[]?$", nil},
			want:    NewOptional(NewArray(map[string]interface{}{"type": "int", "eos.type": "int32"})),
			wantErr: false,
		},
		{
			name:    "int32?->['null','int']",
			args:    args{"int32?", nil},
			want:    NewOptional(map[string]interface{}{"type": "int", "eos.type": "int32"}),
			wantErr: false,
		},
		newTest(
			"uint32",
			"long",
			false,
		),
		newTest(
			"int64",
			"long",
			false,
		),
		{
			name:    "uint64->long",
			args:    args{"uint64", nil},
			want:    map[string]interface{}{"type": "long", "eos.type": "uint64", "logicalType": "eos.uint64"},
			wantErr: false,
		},
		newTest(
			"varint32",
			"int",
			false,
		),
		newTest(
			"varuint32",
			"long",
			false,
		),
		newTest(
			"float32",
			"float",
			false,
		),
		newTest(
			"float64",
			"double",
			false,
		),
		newTest(
			"name",
			"string",
			false,
		),
		newTest(
			"bytes",
			"bytes",
			false,
		),
		newTest(
			"string",
			"string",
			false,
		),
		newTest(
			"checksum160",
			"bytes",
			false,
		),
		newTest(
			"checksum256",
			"bytes",
			false,
		),
		newTest(
			"checksum512",
			"bytes",
			false,
		),
		newTest(
			"symbol_code",
			"string",
			false,
		),
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
						Type: map[string]interface{}{"type": "long", "eos.type": "uint32"},
					},
					{
						Name: "fieldB",
						Type: map[string]interface{}{"type": "long", "eos.type": "int64"},
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
						Type: map[string]interface{}{"type": "string", "eos.type": "string"},
					},
					{
						Name: "parentFieldB",
						Type: map[string]interface{}{"type": "int", "eos.type": "int32"},
					},
					{
						Name: "fieldA",
						Type: map[string]interface{}{"type": "long", "eos.type": "uint32"},
					},
					{
						Name: "fieldB",
						Type: map[string]interface{}{"type": "long", "eos.type": "int64"},
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

func Test_variantResolveFieldTypeSchema(t *testing.T) {
	initBuiltInTypesForTables()
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
			name: "variant->union",
			args: args{"regproducer2", &ABI{&eos.ABI{
				Types: []eos.ABIType{{
					NewTypeName: "block_signing_authority",
					Type:        "variant_block_signing_authority_v0",
				}},
				Structs: []eos.StructDef{{
					Name: "block_signing_authority_v0",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "threshold",
							Type: "uint32",
						},
					},
				}, {
					Name: "regproducer2",
					Base: "",
					Fields: []eos.FieldDef{
						{
							Name: "producer_authority",
							Type: "block_signing_authority",
						},
					},
				},
				},
				Variants: []eos.VariantDef{{
					Name:  "variant_block_signing_authority_v0",
					Types: []string{"block_signing_authority_v0"},
				}},
			}, 42}},
			want: RecordSchema{
				Type: "record",
				Name: "Regproducer2",
				Fields: []FieldSchema{
					{
						Name: "producer_authority",
						Type: RecordSchema{
							Type: "record",
							Name: "BlockSigningAuthorityV0",
							Fields: []FieldSchema{
								{
									Name: "threshold",
									Type: map[string]interface{}{"type": "long", "eos.type": "uint32"},
								},
							},
						},
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
						Type: map[string]interface{}{"type": "long", "eos.type": "uint32"},
					},
					{
						Name: "fieldB",
						Type: map[string]interface{}{"type": "long", "eos.type": "int64"},
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
						Type: map[string]interface{}{"type": "long", "eos.type": "uint32"},
					},
					{
						Name: "fieldB",
						Type: map[string]interface{}{"type": "long", "eos.type": "int64"},
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
