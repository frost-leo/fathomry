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
	"encoding/binary"
	"math"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Keep response-cardinality admission ahead of kmsg's slice allocation. The SDK
// remains the protocol/value decoder; this walker only validates the v13 envelope.
type boundedFetchRequest struct {
	*kmsg.FetchRequest
	wireBytes  int
	maxAborted int
}

func (request *boundedFetchRequest) ResponseKind() kmsg.Response {
	response := kmsg.NewPtrFetchResponse()
	response.SetVersion(request.GetVersion())
	return &boundedFetchResponse{FetchResponse: response, wireBytes: request.wireBytes, maxAborted: request.maxAborted}
}

// kgo recognizes the concrete native FetchRequest specially. Its wrapper must
// preserve the same read timeout through the native TimeoutRequest contract.
func (request *boundedFetchRequest) Timeout() int32         { return request.MaxWaitMillis }
func (request *boundedFetchRequest) SetTimeout(value int32) { request.MaxWaitMillis = value }

type boundedFetchResponse struct {
	*kmsg.FetchResponse
	wireBytes  int
	maxAborted int
}

func (response *boundedFetchResponse) ReadFrom(data []byte) error {
	if err := preflightFetchResponse(data, response.Version, response.wireBytes, response.maxAborted); err != nil {
		return err
	}
	return response.FetchResponse.ReadFrom(data)
}
func (response *boundedFetchResponse) UnsafeReadFrom(data []byte) error {
	if err := preflightFetchResponse(data, response.Version, response.wireBytes, response.maxAborted); err != nil {
		return err
	}
	return response.FetchResponse.UnsafeReadFrom(data)
}

const maxFetchTags = 64

type fetchEnvelope struct {
	data     []byte
	tagsLeft *uint32
	err      error
}

func (frame *fetchEnvelope) take(size uint32) []byte {
	if frame.err != nil {
		return nil
	}
	if uint64(size) > uint64(len(frame.data)) {
		frame.err = failure(ErrRead, "fetch-frame")
		return nil
	}
	value := frame.data[:int(size)]
	frame.data = frame.data[int(size):]
	return value
}
func (frame *fetchEnvelope) integer() uint32 {
	if frame.err != nil {
		return 0
	}
	value, used := binary.Uvarint(frame.data)
	if used <= 0 || used > 5 || value > math.MaxUint32 {
		frame.err = failure(ErrRead, "fetch-integer")
		return 0
	}
	frame.data = frame.data[used:]
	return uint32(value)
}
func (frame *fetchEnvelope) array(maximum uint32, minimumBytes uint32, nullable bool) uint32 {
	length := frame.integer()
	if frame.err != nil {
		return 0
	}
	if length == 0 {
		if !nullable {
			frame.err = failure(ErrRead, "fetch-array")
		}
		return 0
	}
	count := length - 1
	if count > maximum || uint64(count)*uint64(minimumBytes) > uint64(len(frame.data)) {
		frame.err = failure(ErrLimit, "fetch-cardinality")
		return 0
	}
	return count
}
func (frame *fetchEnvelope) compact(nullable bool) {
	length := frame.integer()
	if frame.err != nil {
		return
	}
	if length == 0 {
		if !nullable {
			frame.err = failure(ErrRead, "fetch-bytes")
		}
		return
	}
	frame.take(length - 1)
}
func (frame *fetchEnvelope) complete() error {
	if frame.err != nil {
		return frame.err
	}
	if len(frame.data) != 0 {
		return failure(ErrRead, "fetch-trailing")
	}
	return nil
}

type fetchTagScope uint8

const (
	fetchOpaqueTags fetchTagScope = iota
	fetchPartitionTags
	fetchRootTags
)

func (frame *fetchEnvelope) tags(scope fetchTagScope) {
	count := frame.integer()
	if frame.err != nil {
		return
	}
	if count > *frame.tagsLeft {
		frame.err = failure(ErrLimit, "fetch-tags")
		return
	}
	*frame.tagsLeft -= count
	var previous uint32
	for index := uint32(0); index < count && frame.err == nil; index++ {
		key := frame.integer()
		size := frame.integer()
		payload := frame.take(size)
		if frame.err != nil {
			return
		}
		if index > 0 && key <= previous {
			frame.err = failure(ErrRead, "fetch-tag-order")
			return
		}
		previous = key
		nested := fetchEnvelope{data: payload, tagsLeft: frame.tagsLeft}
		switch {
		case scope == fetchPartitionTags && key <= 2:
			fixed := uint32(12)
			if key == 1 {
				fixed = 8
			}
			nested.take(fixed)
			nested.tags(fetchOpaqueTags)
		case scope == fetchRootTags && key == 0:
			// kmsg parses this known tag even on v13; bound it before delegation.
			brokers := nested.array(16, 11, false)
			for index := uint32(0); index < brokers && nested.err == nil; index++ {
				nested.take(4)
				nested.compact(false)
				nested.take(4)
				nested.compact(true)
				nested.tags(fetchOpaqueTags)
			}
		default:
			continue
		}
		frame.err = nested.complete()
	}
}

func preflightFetchResponse(data []byte, version int16, maximum, maxAborted int) error {
	if version != 13 {
		return failure(ErrUnsupported, "fetch-version")
	}
	if len(data) > maximum {
		return failure(ErrLimit, "fetch-wire")
	}
	if maxAborted < 0 {
		return failure(ErrInput, "fetch-aborts")
	}
	tagsLeft := uint32(maxFetchTags)
	frame := fetchEnvelope{data: data, tagsLeft: &tagsLeft}
	frame.take(10)
	topics := frame.array(1, 18, false)
	for topic := uint32(0); topic < topics && frame.err == nil; topic++ {
		frame.take(16)
		partitions := frame.array(1, 37, false)
		for partition := uint32(0); partition < partitions && frame.err == nil; partition++ {
			frame.take(30)
			aborted := frame.array(uint32(min(maxAborted, maximum/17)), 17, true)
			for index := uint32(0); index < aborted && frame.err == nil; index++ {
				frame.take(16)
				frame.tags(fetchOpaqueTags)
			}
			frame.take(4)
			frame.compact(true)
			frame.tags(fetchPartitionTags)
		}
		frame.tags(fetchOpaqueTags)
	}
	frame.tags(fetchRootTags)
	return frame.complete()
}
