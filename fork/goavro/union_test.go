// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"
)

func TestSchemaUnion(t *testing.T) {
	testSchemaInvalid(t, `[{"type":"enum","name":"e1","symbols":["alpha","bravo"]},"e1"]`, "Union item 2 ought to be unique type")
	testSchemaInvalid(t, `[{"type":"enum","name":"com.example.one","symbols":["red","green","blue"]},{"type":"enum","name":"one","namespace":"com.example","symbols":["dog","cat"]}]`, "Union item 2 ought to be unique type")
}

func TestUnion(t *testing.T) {
	testBinaryCodecPass(t, `["null"]`, Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["null","int"]`, Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["int","null"]`, Union("null", nil), []byte("\x02"))

	testBinaryCodecPass(t, `["null","int"]`, Union("int", 3), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","long"]`, Union("long", 3), []byte("\x02\x06"))

	testBinaryCodecPass(t, `["int","null"]`, Union("int", 3), []byte("\x00\x06"))
	testBinaryEncodePass(t, `["int","null"]`, Union("int", 3), []byte("\x00\x06")) // can encode a bare 3

	testBinaryEncodeFail(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, Union("colors", "bravo"), "value ought to be member of symbols")
	testBinaryEncodeFail(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, Union("animals", "bravo"), "value ought to be member of symbols")
	testBinaryCodecPass(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, Union("colors", "green"), []byte{0, 2})
	testBinaryCodecPass(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, Union("animals", "cat"), []byte{2, 2})

	testUnableUnionPass(t, `["null","int"]`, 3, map[string]interface{}{"int": 3})
}

func TestUnionRejectInvalidType(t *testing.T) {
	// testBinaryEncodeFailBadDatumType(t, `["null","long"]`, 3)
	testBinaryEncodeFailBadDatumType(t, `["null","int","long","float"]`, float64(3.5))
	testBinaryEncodeFailBadDatumType(t, `["null","long"]`, Union("int", 3))
	testBinaryEncodeFailBadDatumType(t, `["null","int","long","float"]`, Union("double", float64(3.5)))
}

func TestUnionWillCoerceTypeIfPossible(t *testing.T) {
	testBinaryCodecPass(t, `["null","long","float","double"]`, Union("long", int32(3)), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","int","float","double"]`, Union("int", int64(3)), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","int","long","double"]`, Union("double", float32(3.5)), []byte("\x06\x00\x00\x00\x00\x00\x00\f@"))
	testBinaryCodecPass(t, `["null","int","long","float"]`, Union("float", float64(3.5)), []byte("\x06\x00\x00\x60\x40"))
}

func TestUnionNumericCoercionGuardsPrecision(t *testing.T) {
	testBinaryEncodeFail(t, `["null","int","long","double"]`, Union("int", float32(3.5)), "lose precision")
}

func TestUnionWithArray(t *testing.T) {
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, Union("null", nil), []byte("\x00"))

	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, Union("array", []interface{}{}), []byte("\x02\x00"))
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, Union("array", []interface{}{1}), []byte("\x02\x02\x02\x00"))
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, Union("array", []interface{}{1, 2}), []byte("\x02\x04\x02\x04\x00"))

	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, Union("null", nil), []byte{2})
	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, Union("array", []string{"foo"}), []byte("\x00\x02\x06foo\x00"))
	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, Union("array", []string{"foo", "bar"}), []byte("\x00\x04\x06foo\x06bar\x00"))
}

func TestUnionWithMap(t *testing.T) {
	testBinaryCodecPass(t, `["null",{"type":"map","values":"string"}]`, Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["string",{"type":"map","values":"string"}]`, Union("map", map[string]interface{}{"He": "Helium"}), []byte("\x02\x02\x04He\x0cHelium\x00"))
	testBinaryCodecPass(t, `["string",{"type":"array","items":"string"}]`, Union("string", "Helium"), []byte("\x00\x0cHelium"))
}

func TestUnionDynamicEncode(t *testing.T) {
	testBinaryEncodePass(t, `["null","long"]`, 3, []byte("\x02\x06"))
	// TODO add more test cases
}

func TestUnionEosEncode(t *testing.T) {
	// output first parameter is type position, second is value

	// Boolean Type
	testBinaryEncodePass(t, `["null","boolean"]`, []interface{}{"bool", true}, []byte("\x02\x01"))
	testBinaryEncodePass(t, `["null","boolean"]`, []interface{}{"bool", 1}, []byte("\x02\x01"))
	testBinaryEncodePass(t, `["null","boolean"]`, []interface{}{"bool", "true"}, []byte("\x02\x01"))
	testBinaryEncodePass(t, `["null","boolean"]`, []interface{}{"bool", false}, []byte("\x02\x00"))
	testBinaryEncodePass(t, `["null","boolean"]`, []interface{}{"bool", 0}, []byte("\x02\x00"))

	// Integer Types (int, uint mapped to int or long as per mapping)
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"int8", int8(5)}, []byte("\x02\x0a"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"int8", "5"}, []byte("\x02\x0a"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"uint8", uint8(5)}, []byte("\x02\x0a"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"uint8", "5"}, []byte("\x02\x0a"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"int16", int16(300)}, []byte("\x02\xd8\x04"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"int16", "300"}, []byte("\x02\xd8\x04"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"uint16", uint16(300)}, []byte("\x02\xd8\x04"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"uint16", "300"}, []byte("\x02\xd8\x04"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"int32", int32(3)}, []byte("\x02\x06"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"int32", "3"}, []byte("\x02\x06"))
	testBinaryEncodePass(t, `["null","long"]`, []interface{}{"uint32", uint32(3)}, []byte("\x02\x06"))
	testBinaryEncodePass(t, `["null","long"]`, []interface{}{"uint32", "3"}, []byte("\x02\x06"))
	testBinaryEncodePass(t, `["null","long"]`, []interface{}{"int64", int64(10)}, []byte("\x02\x14"))
	testBinaryEncodePass(t, `["null","long"]`, []interface{}{"int64", "10"}, []byte("\x02\x14"))
	testBinaryEncodePass(t, `["null","long"]`, []interface{}{"uint64", uint64(10)}, []byte("\x02\x14"))
	testBinaryEncodePass(t, `["null","long"]`, []interface{}{"uint64", "10"}, []byte("\x02\x14"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"varint32", int32(20)}, []byte("\x02\x28"))
	testBinaryEncodePass(t, `["null","int"]`, []interface{}{"varint32", "20"}, []byte("\x02\x28"))
	testBinaryEncodePass(t, `["null","long"]`, []interface{}{"varuint32", uint32(20)}, []byte("\x02\x28"))
	testBinaryEncodePass(t, `["null","long"]`, []interface{}{"varuint32", "20"}, []byte("\x02\x28"))

	// Floating Point Types
	testBinaryEncodePass(t, `["null","float"]`, []interface{}{"float32", "0.12300000339746475"}, []byte("\x02\x6d\xe7\xfb\x3d"))
	testBinaryEncodePass(t, `["null","double"]`, []interface{}{"float64", "0.12300000339746475"}, []byte("\x02\x00\x00\x00\xa0\xed\x7c\xbf\x3f"))
	testBinaryEncodePass(t, `["null","float"]`, []interface{}{"float32", float32(0.123)}, []byte("\x02\x6d\xe7\xfb\x3d"))
	testBinaryEncodePass(t, `["null","double"]`, []interface{}{"float64", float64(0.123)}, []byte("\x02\xb0\x72\x68\x91\xed\x7c\xbf\x3f"))

	// Time and Timestamp Types
	// testBinaryEncodePass(t, `["null","long"]`, []interface{}{"time_point", int64(1609459200)}, []byte("\x02\x80\xea\xf0\x01"))
	// testBinaryEncodePass(t, `["null","long"]`, []interface{}{"time_point_sec", int64(1609459200)}, []byte("\x02\x80\xea\xf0\x01"))
	// testBinaryEncodePass(t, `["null","long"]`, []interface{}{"block_timestamp_type", int64(1609459200)}, []byte("\x02\x80\xea\xf0\x01"))

	// // String Types
	// testBinaryEncodePass(t, `["null","string"]`, []interface{}{"name", "ultra"}, []byte("\x00\x0a\x75\x6c\x74\x72\x61"))
	// testBinaryEncodePass(t, `["null","string"]`, []interface{}{"string", "hello"}, []byte("\x00\x0a\x68\x65\x6c\x6c\x6f"))
	// testBinaryEncodePass(t, `["null","string"]`, []interface{}{"symbol", "USD"}, []byte("\x00\x06\x55\x53\x44"))
	// testBinaryEncodePass(t, `["null","string"]`, []interface{}{"symbol_code", "EUR"}, []byte("\x00\x06\x45\x55\x52"))

	// // Byte/Bytes Types
	// testBinaryEncodePass(t, `["null","bytes"]`, []interface{}{"bytes", []byte{0x01, 0x02, 0x03}}, []byte("\x00\x06\x01\x02\x03"))
	// testBinaryEncodePass(t, `["null","bytes"]`, []interface{}{"checksum160", []byte{0x01, 0x02, 0x03, 0x04, 0x05}}, []byte("\x00\x0a\x01\x02\x03\x04\x05"))
	// testBinaryEncodePass(t, `["null","bytes"]`, []interface{}{"checksum256", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}}, []byte("\x00\x10\x01\x02\x03\x04\x05\x06\x07\x08"))
	// testBinaryEncodePass(t, `["null","bytes"]`, []interface{}{"checksum512", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}}, []byte("\x00\x14\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a"))

	testBinaryEncodePass(t, `["string","int","long"]`, []interface{}{"name", "ultra"}, []byte("\x00\x0a\x75\x6c\x74\x72\x61"))

}

func TestUnionMapRecordFitsInRecord(t *testing.T) {
	// union value may be either map or a record
	codec, err := NewCodec(`["null",{"type":"map","values":"double"},{"type":"record","name":"com.example.record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"float"}]}]`)
	if err != nil {
		t.Fatal(err)
	}

	// the provided datum value could be encoded by either the map or the record
	// schemas above
	datum := map[string]interface{}{
		"field1": 3,
		"field2": 3.5,
	}
	datumIn := Union("com.example.record", datum)

	buf, err := codec.BinaryFromNative(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte{
		0x04,                   // prefer record (union item 2) over map (union item 1)
		0x06,                   // field1 == 3
		0x00, 0x00, 0x60, 0x40, // field2 == 3.5
	}) {
		t.Errorf("GOT: %#v; WANT: %#v", buf, []byte{byte(2)})
	}

	// round trip
	datumOut, buf, err := codec.NativeFromBinary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(buf), 0; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}

	datumOutMap, ok := datumOut.(map[string]interface{})
	if !ok {
		t.Fatalf("GOT: %#v; WANT: %#v", ok, false)
	}
	if actual, expected := len(datumOutMap), 1; actual != expected {
		t.Fatalf("GOT: %#v; WANT: %#v", actual, expected)
	}
	datumValue, ok := datumOutMap["com.example.record"]
	if !ok {
		t.Fatalf("GOT: %#v; WANT: %#v", datumOutMap, "have `com.example.record` key")
	}
	datumValueMap, ok := datumValue.(map[string]interface{})
	if !ok {
		t.Errorf("GOT: %#v; WANT: %#v", ok, true)
	}
	if actual, expected := len(datumValueMap), len(datum); actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	for k, v := range datum {
		if actual, expected := fmt.Sprintf("%v", datumValueMap[k]), fmt.Sprintf("%v", v); actual != expected {
			t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
		}
	}
}

func TestUnionRecordFieldWhenNull(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "r1",
  "fields": [
    {"name": "f1", "type": [{"type": "array", "items": "string"}, "null"]}
  ]
}`

	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": Union("array", []interface{}{})}, []byte("\x00\x00"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": Union("array", []string{"bar"})}, []byte("\x00\x02\x06bar\x00"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": Union("array", []string{})}, []byte("\x00\x00"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": Union("null", nil)}, []byte("\x02"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": nil}, []byte("\x02"))
}

func TestUnionText(t *testing.T) {
	testTextEncodeFail(t, `["null","int"]`, Union("null", 3), "expected")
	testTextCodecPass(t, `["null","int"]`, Union("null", nil), []byte("null"))
	testTextCodecPass(t, `["null","int"]`, Union("int", 3), []byte(`{"int":3}`))
	testTextCodecPass(t, `["null","int","string"]`, Union("string", "😂 "), []byte(`{"string":"\u0001\uD83D\uDE02 "}`))
}

func ExampleUnion() {
	codec, err := NewCodec(`["null","string","int"]`)
	if err != nil {
		fmt.Println(err)
	}
	buf, err := codec.TextualFromNative(nil, Union("string", "some string"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	// Output: {"string":"some string"}
}

func ExampleUnion3() {
	// Imagine a record field with the following union type. I have seen this
	// sort of type in many schemas. I have been told the reasoning behind it is
	// when the writer desires to encode data to JSON that cannot be written as
	// a JSON number, then to encode it as a string and allow the reader to
	// parse the string accordingly.
	codec, err := NewCodec(`["null","double","string"]`)
	if err != nil {
		fmt.Println(err)
	}

	native, _, err := codec.NativeFromTextual([]byte(`{"string":"NaN"}`))
	if err != nil {
		fmt.Println(err)
	}

	value := math.NaN()
	if native == nil {
		fmt.Print("decoded null: ")
	} else {
		for k, v := range native.(map[string]interface{}) {
			switch k {
			case "double":
				fmt.Print("decoded double: ")
				value = v.(float64)
			case "string":
				fmt.Print("decoded string: ")
				s := v.(string)
				switch s {
				case "NaN":
					value = math.NaN()
				case "+Infinity":
					value = math.Inf(1)
				case "-Infinity":
					value = math.Inf(-1)
				default:
					var err error
					value, err = strconv.ParseFloat(s, 64)
					if err != nil {
						fmt.Println(err)
					}
				}
			}
		}
	}
	fmt.Println(value)
	// Output: decoded string: NaN
}

func ExampleJSONUnion() {
	codec, err := NewCodec(`["null","string","int"]`)
	if err != nil {
		fmt.Println(err)
	}
	buf, err := codec.TextualFromNative(nil, Union("string", "some string"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	// Output: {"string":"some string"}
}

//
// The following examples show the way to put a new codec into use
// Currently the only new codec is ont that supports standard json
// which does not indicate unions in any way
// so standard json data needs to be guided into avro unions

// show how to use the default codec via the NewCodecFrom mechanism
func ExampleCustomCodec() {
	codec, err := NewCodecFrom(`"string"`, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySlice,
	}, nil)
	if err != nil {
		fmt.Println(err)
	}
	buf, err := codec.TextualFromNative(nil, "some string 22")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	// Output: "some string 22"
}

// Use the standard JSON codec instead
func ExampleJSONStringToTextual() {
	codec, err := NewCodecFrom(`["null","string","int"]`, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySliceJSON,
	}, nil)
	if err != nil {
		fmt.Println(err)
	}
	buf, err := codec.TextualFromNative(nil, Union("string", "some string"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	// Output: {"string":"some string"}
}

func ExampleJSONStringToNative() {
	codec, err := NewCodecFrom(`["null","string","int"]`, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySliceJSON,
	}, nil)
	if err != nil {
		fmt.Println(err)
	}
	// send in a legit json string
	t, _, err := codec.NativeFromTextual([]byte("\"some string one\""))
	if err != nil {
		fmt.Println(err)
	}
	// see it parse into a map like the avro encoder does
	o, ok := t.(map[string]interface{})
	if !ok {
		fmt.Printf("its a %T not a map[string]interface{}", t)
	}

	// pull out the string to show its all good
	_v := o["string"]
	v, ok := _v.(string)
	fmt.Println(v)
	// Output: some string one
}

func TestUnionJSON(t *testing.T) {
	testJSONDecodePass(t, `["null","int"]`, nil, []byte("null"))
	testJSONDecodePass(t, `["null","int","long"]`, Union("int", 3), []byte(`3`))
	testJSONDecodePass(t, `["null","long","int"]`, Union("int", 3), []byte(`3`))
	testJSONDecodePass(t, `["null","int","long"]`, Union("long", 333333333333333), []byte(`333333333333333`))
	testJSONDecodePass(t, `["null","long","int"]`, Union("long", 333333333333333), []byte(`333333333333333`))
	testJSONDecodePass(t, `["null","float","int","long"]`, Union("float", 6.77), []byte(`6.77`))
	testJSONDecodePass(t, `["null","int","float","long"]`, Union("float", 6.77), []byte(`6.77`))
	testJSONDecodePass(t, `["null","double","int","long"]`, Union("double", 6.77), []byte(`6.77`))
	testJSONDecodePass(t, `["null","int","float","double","long"]`, Union("double", 6.77), []byte(`6.77`))
	testJSONDecodePass(t, `["null",{"type":"array","items":"int"}]`, Union("array", []interface{}{1, 2}), []byte(`[1,2]`))
	testJSONDecodePass(t, `["null",{"type":"map","values":"int"}]`, Union("map", map[string]interface{}{"k1": 13}), []byte(`{"k1":13}`))
	testJSONDecodePass(t, `["null",{"name":"r1","type":"record","fields":[{"name":"field1","type":"string"},{"name":"field2","type":"string"}]}]`, Union("r1", map[string]interface{}{"field1": "value1", "field2": "value2"}), []byte(`{"field1": "value1", "field2": "value2"}`))
	testJSONDecodePass(t, `["null","boolean"]`, Union("boolean", true), []byte(`true`))
	testJSONDecodePass(t, `["null","boolean"]`, Union("boolean", false), []byte(`false`))
	testJSONDecodePass(t, `["null",{"type":"enum","name":"e1","symbols":["alpha","bravo"]}]`, Union("e1", "bravo"), []byte(`"bravo"`))
	testJSONDecodePass(t, `["null", "bytes"]`, Union("bytes", []byte("")), []byte("\"\""))
	testJSONDecodePass(t, `["null", "bytes", "string"]`, Union("bytes", []byte("")), []byte("\"\""))
	testJSONDecodePass(t, `["null", "string", "bytes"]`, Union("string", "value1"), []byte(`"value1"`))
	testJSONDecodePass(t, `["null", {"type":"enum","name":"e1","symbols":["alpha","bravo"]}, "string"]`, Union("e1", "bravo"), []byte(`"bravo"`))
	testJSONDecodePass(t, `["null", {"type":"fixed","name":"f1","size":4}]`, Union("f1", []byte(`abcd`)), []byte(`"abcd"`))
	testJSONDecodePass(t, `"string"`, "abcd", []byte(`"abcd"`))
	testJSONDecodePass(t, `{"type":"record","name":"kubeEvents","fields":[{"name":"field1","type":"string","default":""}]}`, map[string]interface{}{"field1": "value1"}, []byte(`{"field1":"value1"}`))
	testJSONDecodePass(t, `{"type":"record","name":"kubeEvents","fields":[{"name":"field1","type":"string","default":""},{"name":"field2","type":"string"}]}`, map[string]interface{}{"field1": "", "field2": "deef"}, []byte(`{"field2": "deef"}`))
	testJSONDecodePass(t, `{"type":"record","name":"kubeEvents","fields":[{"name":"field1","type":["string","null"],"default":""}]}`, map[string]interface{}{"field1": Union("string", "value1")}, []byte(`{"field1":"value1"}`))
	testJSONDecodePass(t, `{"type":"record","name":"kubeEvents","fields":[{"name":"field1","type":["string","null"],"default":""}]}`, map[string]interface{}{"field1": nil}, []byte(`{"field1":null}`))
	// union of null which has minimal syntax
	testJSONDecodePass(t, `{"type":"record","name":"LongList","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": nil}, []byte(`{"next": null}`))
	// record containing union of record (recursive record)
	testJSONDecodePass(t, `{"type":"record","name":"LongList","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": Union("LongList", map[string]interface{}{"next": nil})}, []byte(`{"next":{"next":null}}`))
	testJSONDecodePass(t, `{"type":"record","name":"LongList","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": Union("LongList", map[string]interface{}{"next": Union("LongList", map[string]interface{}{"next": nil})})}, []byte(`{"next":{"next":{"next":null}}}`))
}

func testUnableUnionPass(t *testing.T, schema string, datum interface{}, expectedDatum interface{}) {
	t.Helper()
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatalf("Schema: %q %s", schema, err)
	}

	actualBinary, err := codec.BinaryFromNative(nil, datum)
	if err != nil {
		t.Fatalf("schema: %s; Datum: %v; %s", schema, datum, err)
	}

	value, remaining, err := codec.NativeFromBinary(actualBinary)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be
	// decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}
	// for testing purposes, to prevent big switch statement, convert each to
	// string and compare.
	if actual, expected := fmt.Sprintf("%v", value), fmt.Sprintf("%v", expectedDatum); actual != expected {
		t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}
}
