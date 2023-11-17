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
	"math"
	"testing"
)

func TestSchemaPrimitiveCodecInt(t *testing.T) {
	testSchemaPrimativeCodec(t, `"int"`)
}

type Tint int
type Tint32 int32
type Tint64 int64
type TUint uint
type TUint32 uint32
type TUint64 uint64

func TestPrimitiveIntBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, `"int"`, "some string")
	testBinaryDecodeFailShortBuffer(t, `"int"`, []byte{0xfd, 0xff, 0xff, 0xff})
	testBinaryCodecPass(t, `"int"`, -1, []byte{0x01})
	testBinaryCodecPass(t, `"int"`, -2147483647, []byte{0xfd, 0xff, 0xff, 0xff, 0xf})
	testBinaryCodecPass(t, `"int"`, -3, []byte{0x05})
	testBinaryCodecPass(t, `"int"`, -65, []byte("\x81\x01"))
	testBinaryCodecPass(t, `"int"`, 0, []byte{0x00})
	testBinaryCodecPass(t, `"int"`, 1, []byte{0x02})
	testBinaryCodecPass(t, `"int"`, 1016, []byte("\xf0\x0f"))
	testBinaryCodecPass(t, `"int"`, 1455301406, []byte{0xbc, 0x8c, 0xf1, 0xeb, 0xa})
	testBinaryCodecPass(t, `"int"`, 2147483647, []byte{0xfe, 0xff, 0xff, 0xff, 0xf})
	testBinaryCodecPass(t, `"int"`, 3, []byte("\x06"))
	testBinaryCodecPass(t, `"int"`, 64, []byte("\x80\x01"))
	testBinaryCodecPass(t, `"int"`, 66052, []byte("\x88\x88\x08"))
	testBinaryCodecPass(t, `"int"`, 8454660, []byte("\x88\x88\x88\x08"))
	testBinaryCodecPass(t, `"int"`, "8454660", []byte("\x88\x88\x88\x08"))

	// uint(32|64) support
	testBinaryCodecPass(t, `"int"`, uint32(64), []byte("\x80\x01"))
	testBinaryWriteReadPass(t, `"int"`, uint32(math.MaxInt32))
	testBinaryWriteReadPass(t, `"int"`, uint64(math.MaxInt32))
	testBinaryEncodeFail(t, `"int"`, uint32(math.MaxInt32+1), "uint32 would lose precision")
	testBinaryEncodeFail(t, `"int"`, uint64(math.MaxInt32+1), "uint32 would lose precision")
	// reflect
	testBinaryCodecPass(t, `"int"`, TUint(64), []byte("\x80\x01"))
	testBinaryCodecPass(t, `"int"`, TUint32(64), []byte("\x80\x01"))
	testBinaryCodecPass(t, `"int"`, TUint64(64), []byte("\x80\x01"))
	testBinaryEncodeFail(t, `"int"`, TUint32(math.MaxInt32+1), "goavro.TUint32 would lose precision")
	testBinaryEncodeFail(t, `"int"`, TUint64(math.MaxInt32+1), "goavro.TUint64 would lose precision")
	testBinaryCodecPass(t, `"int"`, Tint(64), []byte("\x80\x01"))
	testBinaryCodecPass(t, `"int"`, Tint32(64), []byte("\x80\x01"))
	testBinaryCodecPass(t, `"int"`, Tint64(64), []byte("\x80\x01"))

	testBinaryCodecPass(t, `"long"`, TUint(64), []byte("\x80\x01"))
	testBinaryCodecPass(t, `"long"`, TUint32(64), []byte("\x80\x01"))
	testBinaryCodecPass(t, `"long"`, TUint64(64), []byte("\x80\x01"))
	testBinaryEncodeFail(t, `"long"`, TUint64(math.MaxInt64+1), "goavro.TUint64 would lose precision")
	testBinaryCodecPass(t, `"long"`, Tint(64), []byte("\x80\x01"))
	testBinaryCodecPass(t, `"long"`, Tint32(64), []byte("\x80\x01"))
	testBinaryCodecPass(t, `"long"`, Tint64(64), []byte("\x80\x01"))
}

func TestPrimitiveIntText(t *testing.T) {
	testTextDecodeFailShortBuffer(t, `"int"`, []byte(""))
	testTextDecodeFailShortBuffer(t, `"int"`, []byte("-"))

	testTextCodecPass(t, `"int"`, -13, []byte("-13"))
	testTextCodecPass(t, `"int"`, 0, []byte("0"))
	testTextCodecPass(t, `"int"`, 13, []byte("13"))
	testTextDecodePass(t, `"int"`, -0, []byte("-0"))
	testTextEncodePass(t, `"int"`, -0, []byte("0")) // NOTE: -0 encodes as "0"
}

func TestSchemaPrimitiveCodecLong(t *testing.T) {
	testSchemaPrimativeCodec(t, `"long"`)
}

func TestPrimitiveLongBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, `"long"`, "some string")
	testBinaryDecodeFailShortBuffer(t, `"long"`, []byte("\xff\xff\xff\xff"))
	testBinaryCodecPass(t, `"long"`, int64((1<<63)-1), []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1})
	testBinaryCodecPass(t, `"long"`, int64(-(1 << 63)), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1})
	testBinaryCodecPass(t, `"long"`, -2147483648, []byte("\xff\xff\xff\xff\x0f"))
	testBinaryCodecPass(t, `"long"`, -3, []byte("\x05"))
	testBinaryCodecPass(t, `"long"`, -65, []byte("\x81\x01"))
	testBinaryCodecPass(t, `"long"`, 0, []byte("\x00"))
	testBinaryCodecPass(t, `"long"`, 1082196484, []byte("\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, `"long"`, int64(1359702038045356208), []byte{0xe0, 0xc2, 0x8b, 0xa1, 0x96, 0xf3, 0xd0, 0xde, 0x25})
	testBinaryCodecPass(t, `"long"`, int64(138521149956), []byte("\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, `"long"`, int64(17730707194372), []byte("\x88\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, `"long"`, 2147483647, []byte("\xfe\xff\xff\xff\x0f"))
	testBinaryCodecPass(t, `"long"`, int64(2269530520879620), []byte("\x88\x88\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, `"long"`, 3, []byte("\x06"))
	testBinaryCodecPass(t, `"long"`, int64(5959107741628848600), []byte{0xb0, 0xe7, 0x8a, 0xe1, 0xe2, 0xba, 0x80, 0xb3, 0xa5, 0x1})
	testBinaryCodecPass(t, `"long"`, 64, []byte("\x80\x01"))
	testBinaryCodecPass(t, `"long"`, "64", []byte("\x80\x01"))

	// https://github.com/linkedin/goavro/issues/49
	testBinaryCodecPass(t, `"long"`, int64(-5513458701470791632), []byte("\x9f\xdf\x9f\x8f\xc7\xde\xde\x83\x99\x01"))
	testBinaryCodecPass(t, `"long"`, uint32(64), []byte("\x80\x01"))
	testBinaryWriteReadPass(t, `"long"`, uint32(64))

	// uint(32|64) support
	testBinaryWriteReadPass(t, `"long"`, uint32(math.MaxUint32))
	testBinaryWriteReadPass(t, `"long"`, uint64(math.MaxInt64))
	// testBinaryEncodeFail(t, `"long"`, uint64(math.MaxInt64+1), "uint64 would lose precision")
}

func TestPrimitiveLongText(t *testing.T) {
	testTextDecodeFailShortBuffer(t, `"long"`, []byte(""))
	testTextDecodeFailShortBuffer(t, `"long"`, []byte("-"))

	testTextCodecPass(t, `"long"`, -13, []byte("-13"))
	testTextCodecPass(t, `"long"`, 0, []byte("0"))
	testTextCodecPass(t, `"long"`, 13, []byte("13"))
	testTextDecodePass(t, `"long"`, -0, []byte("-0"))
	testTextEncodePass(t, `"long"`, -0, []byte("0")) // NOTE: -0 encodes as "0"
}
