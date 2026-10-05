/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package franz

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCoreCodecWireInteroperability(t *testing.T) {
	for codec, name := range []string{"none", "gzip", "snappy", "lz4", "zstd"} {
		t.Run(name, func(t *testing.T) {
			cluster := localCluster(t)
			options := clusterOptions(cluster)
			options.Compression = name
			fixture := bindFixture(t, options, 3)
			var wireSeen, wireMismatch atomic.Bool
			cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
				produce := request.(*kmsg.ProduceRequest)
				var batch kmsg.RecordBatch
				observed := -1
				if len(produce.Topics) == 1 && len(produce.Topics[0].Partitions) == 1 && batch.ReadFrom(produce.Topics[0].Partitions[0].Records) == nil {
					observed = int(batch.Attributes & 7)
				}
				wireSeen.Store(true)
				if observed != codec {
					wireMismatch.Store(true)
				}
				return nil, nil, false
			})
			payload := bytes.Repeat([]byte("core-codec-payload-"), 128)
			written := produce(t, fixture.client, "codec", Message{Topic: "records", Value: payload, Headers: []Header{{Key: "repeat"}, {Key: "repeat", Value: []byte{}}}})
			position := written.WritesCopy()[0].Position
			if !wireSeen.Load() || wireMismatch.Load() {
				t.Fatal("requested codec did not reach the producer wire")
			}
			independent := observe(t, cluster.ListenAddrs(), position.Topic, position.Partition, position.Offset, 1)[0]
			if !bytes.Equal(independent.Value, payload) {
				t.Fatal("native consumer disagreed with production")
			}
			read := readResult(t, fixture.client, "bounded-read", position)
			if read.State != ReadFound || !bytes.Equal(read.Record.ValueCopy(), payload) {
				t.Fatal("bounded reader disagreed with broker", read.Err)
			}
		})
	}
}

func compressedBatch(t testing.TB, codec kgo.CompressionCodec, records []byte, count int32) []byte {
	t.Helper()
	compressor, err := kgo.DefaultCompressor(codec)
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	encoded, used := compressor.Compress(&buffer, records)
	raw := testBatch(0, count-1, count, encoded)
	binary.BigEndian.PutUint16(raw[21:23], uint16(used))
	binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	return raw
}

func TestCodecPreflightRejectsExpansionCRCAndHeaders(t *testing.T) {
	for _, codec := range []kgo.CompressionCodec{kgo.GzipCompression(), kgo.SnappyCompression(), kgo.Lz4Compression(), kgo.ZstdCompression()} {
		raw := compressedBatch(t, codec, testRecord(bytes.Repeat([]byte("x"), 32<<10)), 1)
		value := defaults(OptionsV1{})
		value.MaxDecodedBatchBytes = 1024
		if _, _, err := prepareFetch(value, raw); !errors.Is(err, ErrLimit) {
			t.Fatalf("expansion accepted: %v", err)
		}
		value.MaxDecodedBatchBytes = 1 << 20
		raw[len(raw)-1] ^= 1
		if _, _, err := prepareFetch(value, raw); !errors.Is(err, ErrRead) {
			t.Fatal("CRC fault accepted")
		}
		body := []byte{0, 0, 0, 1, 1}
		body = binary.AppendVarint(body, 65)
		records := append(binary.AppendVarint(nil, int64(len(body))), body...)
		raw = compressedBatch(t, codec, records, 1)
		// Preflight includes control records; native filtering must not bypass it.
		binary.BigEndian.PutUint16(raw[21:23], binary.BigEndian.Uint16(raw[21:23])|0x20)
		binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
		if _, _, err := prepareFetch(value, raw); !errors.Is(err, ErrLimit) {
			t.Fatal("control header bound bypassed", err)
		}
		value.MaxDecodedRecords = 0
		if _, _, err := prepareFetch(value, raw); !errors.Is(err, ErrLimit) {
			t.Fatal("record-count bound bypassed", err)
		}
	}
}

func TestSnappyXerialFraming(t *testing.T) {
	compressor, _ := kgo.DefaultCompressor(kgo.SnappyCompression())
	var scratch bytes.Buffer
	input := []byte("xerial")
	chunk, _ := compressor.Compress(&scratch, input)
	frame := append([]byte{}, xerialMagic...)
	frame = binary.BigEndian.AppendUint32(frame, 1)
	frame = binary.BigEndian.AppendUint32(frame, 1)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(chunk)))
	frame = append(frame, chunk...)
	decoded, err := expandSnappy(frame, len(input))
	if err != nil || !bytes.Equal(decoded, input) {
		t.Fatal("valid xerial rejected", err)
	}
	if _, err := expandSnappy(frame, len(input)-1); !errors.Is(err, ErrLimit) {
		t.Fatal("xerial expansion accepted")
	}
	for _, bad := range [][]byte{frame[:15], frame[:len(frame)-1], append(append([]byte{}, frame...), 1)} {
		if _, err := expandSnappy(bad, 1024); err == nil {
			t.Fatal("invalid xerial accepted")
		}
	}
}

func FuzzCoreCodecPreflight(f *testing.F) {
	f.Add([]byte{0}, uint8(2))
	f.Add(testRecord([]byte("seed")), uint8(0))
	f.Fuzz(func(t *testing.T, payload []byte, codec uint8) {
		if len(payload) > 64<<10 {
			t.Skip()
		}
		raw := testBatch(0, 0, 1, payload)
		binary.BigEndian.PutUint16(raw[21:23], uint16(codec%8))
		binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
		value := defaults(OptionsV1{})
		value.MaxDecodedBatchBytes = 64 << 10
		value.MaxDecodedRecords = 16
		_, _, _ = prepareFetch(value, raw)
	})
}

func TestSnappyLengthAdmissionPrecedesPayloadDecode(t *testing.T) {
	advertised := binary.AppendUvarint(nil, 32<<20)
	if length, err := s2.DecodedLen(advertised); err != nil || length != 32<<20 {
		t.Fatal("independent claimed-length oracle changed")
	}
	if output, err := expandSnappy(advertised, 1024); output != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("snappy payload decoding preceded advertised-length admission", err)
	}
}

func TestLZ4RefusesLegacyBeforeWorkspaceAllocation(t *testing.T) {
	legacy := binary.LittleEndian.AppendUint32(nil, 0x184c2102)
	repeated := bytes.Repeat(legacy, 100000)
	for _, frame := range [][]byte{legacy, repeated} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		output, err := expandCodec(frame, kgo.CodecLz4, 1536)
		runtime.ReadMemStats(&after)
		if output != nil || !errors.Is(err, ErrUnsupported) {
			t.Fatal("legacy LZ4 reached the native decoder", err)
		}
		if after.TotalAlloc-before.TotalAlloc > 1<<20 {
			t.Fatal("rejected legacy frame allocated native workspace")
		}
	}
}
func TestLZ4ModernFrameProfilesAndTrailingRefusal(t *testing.T) {
	input := bytes.Repeat([]byte("bounded-modern-frame"), 100)
	for _, size := range []lz4.BlockSize{lz4.Block64Kb, lz4.Block256Kb, lz4.Block1Mb, lz4.Block4Mb} {
		var encoded bytes.Buffer
		writer := lz4.NewWriter(&encoded)
		if err := writer.Apply(lz4.BlockSizeOption(size), lz4.SizeOption(uint64(len(input))), lz4.BlockChecksumOption(true), lz4.ChecksumOption(true)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(input); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		wire := encoded.Bytes()
		output, err := expandCodec(wire, kgo.CodecLz4, len(input))
		if err != nil || !bytes.Equal(output, input) {
			t.Fatal("valid native LZ4 frame refused", err)
		}
		if _, err := expandCodec(wire, kgo.CodecLz4, len(input)-1); !errors.Is(err, ErrLimit) {
			t.Fatal("advertised content bound bypassed", err)
		}
		for _, trailing := range [][]byte{binary.LittleEndian.AppendUint32(nil, 0x184c2102), wire} {
			frame := append(bytes.Clone(wire), trailing...)
			if _, err := expandCodec(frame, kgo.CodecLz4, 2*len(input)); !errors.Is(err, ErrUnsupported) {
				t.Fatal("trailing frame could change native workspace", err)
			}
		}
		truncated := wire[:len(wire)-1]
		if _, err := expandCodec(truncated, kgo.CodecLz4, len(input)); !errors.Is(err, ErrRead) {
			t.Fatal("truncated frame accepted")
		}
	}
}

func TestModernLZ4EmptyUncompressedBlock(t *testing.T) {
	payload := []byte("valid-modern-empty-block")
	var buffer bytes.Buffer
	writer := lz4.NewWriter(&buffer)
	if err := writer.Apply(lz4.ChecksumOption(false), lz4.ConcurrencyOption(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	wire := buffer.Bytes()
	withEmptyBlock := append([]byte{}, wire[:len(wire)-4]...)
	withEmptyBlock = binary.LittleEndian.AppendUint32(withEmptyBlock, 0x80000000)
	withEmptyBlock = append(withEmptyBlock, wire[len(wire)-4:]...)
	reader := lz4.NewReader(bytes.NewReader(withEmptyBlock))
	if err := reader.Apply(lz4.ConcurrencyOption(1)); err != nil {
		t.Fatal(err)
	}
	defer reader.Reset(nil)
	independent, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(independent, payload) {
		t.Fatal("selected native decoder did not accept its upstream supported frame", err)
	}
	decoded, err := expandCodec(withEmptyBlock, kgo.CodecLz4, len(payload))
	if err != nil || !bytes.Equal(decoded, payload) {
		t.Fatal("bounded decoder refused a valid modern zero-length uncompressed block", err)
	}
}
