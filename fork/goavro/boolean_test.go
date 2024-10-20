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
	"encoding/binary"
	"testing"
)

func Float64ToByte(f float64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, f)
	return buf.Bytes()
}

func TestSchemaPrimitiveCodecBoolean(t *testing.T) {
	testSchemaPrimativeCodec(t, `"boolean"`)
}

func TestPrimitiveBooleanBinary(t *testing.T) {
	testBinaryEncodePass(t, `"boolean"`, 0, []byte{0})
	testBinaryEncodePass(t, `"boolean"`, 1, []byte{1})
	testBinaryEncodePass(t, `"boolean"`, 2, []byte{1})
	testBinaryEncodeFail(t, `"boolean"`, -1, "cannot encode binary boolean: provided Go numeric is not equal or superior to 0: -1")
	testBinaryEncodePass(t, `"boolean"`, 42.0, []byte{1})
	testBinaryDecodeFailShortBuffer(t, `"boolean"`, nil)
	testBinaryCodecPass(t, `"boolean"`, false, []byte{0})
	testBinaryCodecPass(t, `"boolean"`, true, []byte{1})
	testBinaryEncodePass(t, `"boolean"`, float64(0), []byte{0})
	testBinaryEncodePass(t, `"boolean"`, float64(1), []byte{1})
}

func TestPrimitiveBooleanText(t *testing.T) {
	testTextEncodeFailBadDatumType(t, `"boolean"`, 0)
	testTextEncodeFailBadDatumType(t, `"boolean"`, 1)
	testTextDecodeFailShortBuffer(t, `"boolean"`, nil)
	testTextCodecPass(t, `"boolean"`, false, []byte("false"))
	testTextCodecPass(t, `"boolean"`, true, []byte("true"))
}
