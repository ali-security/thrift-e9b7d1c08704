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
	"compress/zlib"
	"context"
	"io"
	"testing"
)

func TestZlibTransport(t *testing.T) {
	trans, err := NewTZlibTransport(NewTMemoryBuffer(), zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}

type DummyTransportFactory struct{}

func (p *DummyTransportFactory) GetTransport(trans TTransport) (TTransport, error) {
	return NewTMemoryBuffer(), nil
}

func TestZlibFactoryTransportWithFactory(t *testing.T) {
	factory := NewTZlibTransportFactoryWithFactory(
		zlib.BestCompression,
		&DummyTransportFactory{},
	)
	buffer := NewTMemoryBuffer()
	trans, err := factory.GetTransport(buffer)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}

func TestZlibFactoryTransportWithoutFactory(t *testing.T) {
	factory := NewTZlibTransportFactoryWithFactory(zlib.BestCompression, nil)
	buffer := NewTMemoryBuffer()
	trans, err := factory.GetTransport(buffer)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}

func TestZlibTransportMessageSizeLimit(t *testing.T) {
	const dataSize = 4096
	data := bytes.Repeat([]byte{'a'}, dataSize)

	// Write and flush (not close) so TMemoryBuffer.Close() doesn't wipe the data.
	writeBuf := NewTMemoryBuffer()
	writer, err := NewTZlibTransport(writeBuf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	compressed := make([]byte, writeBuf.Len())
	copy(compressed, writeBuf.Bytes())

	readBuf := NewTMemoryBuffer()
	readBuf.Write(compressed)
	reader, err := NewTZlibTransport(readBuf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetTConfiguration(&TConfiguration{MaxMessageSize: 1024})

	_, err = reader.Read(make([]byte, dataSize))
	if err == nil {
		t.Fatal("expected SIZE_LIMIT error, got nil")
	}
	protoEx, ok := err.(TProtocolException)
	if !ok || protoEx.TypeId() != SIZE_LIMIT {
		t.Fatalf("expected SIZE_LIMIT TProtocolException, got %T: %v", err, err)
	}
}

// zlibCompressForTest compresses data through a TZlibTransport and returns a
// fresh TMemoryBuffer holding the compressed bytes.
func zlibCompressForTest(t *testing.T, data []byte) *TMemoryBuffer {
	t.Helper()
	// Write and flush (not close) so TMemoryBuffer.Close() doesn't wipe the data.
	writeBuf := NewTMemoryBuffer()
	writer, err := NewTZlibTransport(writeBuf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	readBuf := NewTMemoryBuffer()
	readBuf.Write(writeBuf.Bytes())
	return readBuf
}

// TestZlibTransportDecompressionBombChunked feeds a highly compressible
// payload that is read back in small chunks: every single Read stays below
// MaxMessageSize, so only cumulative accounting of the decompressed bytes can
// stop the amplification.
func TestZlibTransportDecompressionBombChunked(t *testing.T) {
	const (
		maxSize   = 64 * 1024
		chunkSize = 4096
		bombSize  = 8 * 1024 * 1024
	)

	readBuf := zlibCompressForTest(t, make([]byte, bombSize))
	if readBuf.Len() >= maxSize {
		t.Fatalf("compressed payload is %d bytes, expected less than %d", readBuf.Len(), maxSize)
	}

	reader, err := NewTZlibTransport(readBuf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetTConfiguration(&TConfiguration{MaxMessageSize: maxSize})

	buf := make([]byte, chunkSize)
	total := 0
	for i := 0; i <= bombSize/chunkSize+1; i++ {
		var n int
		n, err = reader.Read(buf)
		total += n
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatalf("expected SIZE_LIMIT error, got nil after reading %d bytes", total)
	}
	protoEx, ok := err.(TProtocolException)
	if !ok || protoEx.TypeId() != SIZE_LIMIT {
		t.Fatalf("expected SIZE_LIMIT TProtocolException after reading %d bytes, got %T: %v", total, err, err)
	}
	if total > maxSize+chunkSize {
		t.Fatalf("decompressed %d bytes before the limit was enforced, expected at most %d", total, maxSize+chunkSize)
	}
}

// TestZlibTransportMessageSizeLimitWithinLimit ensures payloads that fit in
// MaxMessageSize are still fully readable.
func TestZlibTransportMessageSizeLimitWithinLimit(t *testing.T) {
	const maxSize = 64 * 1024
	data := bytes.Repeat([]byte{'a'}, maxSize)

	reader, err := NewTZlibTransport(zlibCompressForTest(t, data), zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetTConfiguration(&TConfiguration{MaxMessageSize: maxSize})

	got := make([]byte, len(data))
	if _, err := io.ReadFull(reader, got); err != nil {
		t.Fatalf("unexpected error reading %d bytes within limit: %v", len(data), err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("decompressed data does not match the original payload")
	}
}
