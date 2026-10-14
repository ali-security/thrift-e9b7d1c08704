/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestCompactProtocolVarintRejectsOverlong(t *testing.T) {
	// 11 continuation bytes (bit 7 set), no terminating byte
	payload := bytes.Repeat([]byte{0x80}, 11)
	trans := NewTMemoryBufferLen(len(payload))
	trans.Write(payload)
	p := NewTCompactProtocol(trans)
	_, err := p.readVarint64()
	if err == nil {
		t.Fatal("expected error for varint over 10 bytes, got nil")
	}
}

func TestCompactProtocolVarintAcceptsValid10Byte(t *testing.T) {
	// 9 continuation bytes followed by a terminating byte
	payload := append(bytes.Repeat([]byte{0x80}, 9), 0x01)
	trans := NewTMemoryBufferLen(len(payload))
	trans.Write(payload)
	p := NewTCompactProtocol(trans)
	_, err := p.readVarint64()
	if err != nil {
		t.Fatalf("unexpected error for valid 10-byte varint: %v", err)
	}
}

// errEndlessVarintReaderExhausted is returned by endlessVarintReader once its
// byte budget is spent. Seeing it means the varint decoder kept consuming input.
var errEndlessVarintReaderExhausted = errors.New("endless varint reader exhausted")

// endlessVarintReader emulates a malicious peer that streams varint
// continuation bytes (MSB set) without ever sending a terminating byte.
// It gives up after remaining bytes so a regression fails instead of hanging.
type endlessVarintReader struct {
	remaining int
}

func (r *endlessVarintReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, errEndlessVarintReaderExhausted
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 0x80
	}
	r.remaining -= n
	return n, nil
}

func assertVarintInvalidData(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error for overlong varint, got nil")
	}
	if errors.Is(err, errEndlessVarintReaderExhausted) {
		t.Fatalf("varint decoder kept reading past 10 bytes: %v", err)
	}
	var pe TProtocolException
	if !errors.As(err, &pe) {
		t.Fatalf("expected TProtocolException, got %T: %v", err, err)
	}
	if pe.TypeId() != INVALID_DATA {
		t.Fatalf("expected TProtocolException type INVALID_DATA (%d), got %d: %v", INVALID_DATA, pe.TypeId(), err)
	}
}

func TestCompactProtocolVarintRejectsEndlessStream(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name string
		read func(p *TCompactProtocol) error
	}{
		{"ReadI16", func(p *TCompactProtocol) error { _, err := p.ReadI16(ctx); return err }},
		{"ReadI32", func(p *TCompactProtocol) error { _, err := p.ReadI32(ctx); return err }},
		{"ReadI64", func(p *TCompactProtocol) error { _, err := p.ReadI64(ctx); return err }},
		{"ReadString", func(p *TCompactProtocol) error { _, err := p.ReadString(ctx); return err }},
		{"ReadBinary", func(p *TCompactProtocol) error { _, err := p.ReadBinary(ctx); return err }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := &endlessVarintReader{remaining: 1 << 20}
			p := NewTCompactProtocol(NewStreamTransportR(r))
			assertVarintInvalidData(t, c.read(p))
		})
	}
}

func TestCompactProtocolVarintRejectsTerminatedOverlong(t *testing.T) {
	// 10 continuation bytes followed by a terminating byte: 11 bytes on the
	// wire, one more than any valid 64-bit varint can occupy.
	payload := append(bytes.Repeat([]byte{0x80}, 10), 0x01)
	trans := NewTMemoryBufferLen(len(payload))
	trans.Write(payload)
	p := NewTCompactProtocol(trans)
	_, err := p.ReadI64(context.Background())
	assertVarintInvalidData(t, err)
}

func TestReadWriteCompactProtocol(t *testing.T) {
	ReadWriteProtocolTest(t, NewTCompactProtocolFactory())

	transports := []TTransport{
		NewTMemoryBuffer(),
		NewStreamTransportRW(bytes.NewBuffer(make([]byte, 0, 16384))),
		NewTFramedTransport(NewTMemoryBuffer()),
	}

	newTZlibTransport := func(trans TTransport, level int) *TZlibTransport {
		t.Helper()
		zlibTrans, err := NewTZlibTransport(trans, level)
		if err != nil {
			t.Fatalf("NewTZlibTransport returned error: %v", err)
		}
		return zlibTrans
	}

	zlib0 := newTZlibTransport(NewTMemoryBuffer(), 0)
	zlib6 := newTZlibTransport(NewTMemoryBuffer(), 6)
	zlib9 := newTZlibTransport(NewTFramedTransport(NewTMemoryBuffer()), 9)
	transports = append(transports, zlib0, zlib6, zlib9)

	for _, trans := range transports {
		p := NewTCompactProtocol(trans)
		ReadWriteBool(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteByte(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteI16(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteI32(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteI64(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteDouble(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteString(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteBinary(t, p, trans)
		trans.Close()
	}
}
