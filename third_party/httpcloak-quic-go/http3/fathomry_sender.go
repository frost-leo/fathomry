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

package http3

import (
	"errors"
	"io"
	"sync"
)

const FathomryCompatibilityRevision = "httpcloak-quic-v1"

type requestSender struct {
	abortWrite func()
	body       io.ReadCloser
	once       sync.Once
	closeErr   error
	sendErr    error
	done       chan struct{}
	owner      *ClientConn
}

func (sender *requestSender) Read(data []byte) (int, error) { return sender.body.Read(data) }
func (sender *requestSender) Close() error {
	sender.once.Do(func() { sender.closeErr = sender.body.Close() })
	return sender.closeErr
}
func (sender *requestSender) finish(err error) {
	sender.sendErr = errors.Join(err, sender.Close())
	sender.owner.senderMu.Lock()
	delete(sender.owner.senders, sender)
	close(sender.done)
	sender.owner.senderMu.Unlock()
}
func (client *ClientConn) startSender(body io.ReadCloser) (*requestSender, error) {
	client.senderMu.Lock()
	defer client.senderMu.Unlock()
	if client.sendersClosed {
		return nil, errors.New("http3: request senders closed")
	}
	if client.senders == nil {
		client.senders = make(map[*requestSender]struct{})
	}
	sender := &requestSender{body: body, owner: client, done: make(chan struct{})}
	client.senders[sender] = struct{}{}
	return sender, nil
}

// FathomryCloseSenders interrupts and joins every registered body/trace/trailer
// sender. Callers must first close the QUIC connection. A non-cooperating input
// or callback can delay completion; returning from Close never fabricates a join.
func (client *ClientConn) FathomryCloseSenders() error {
	client.senderMu.Lock()
	client.sendersClosed = true
	senders := make([]*requestSender, 0, len(client.senders))
	for sender := range client.senders {
		senders = append(senders, sender)
	}
	client.senderMu.Unlock()
	var causes []error
	for _, sender := range senders {
		causes = append(causes, sender.Close())
	}
	for _, sender := range senders {
		<-sender.done
		causes = append(causes, sender.sendErr)
	}
	return errors.Join(causes...)
}

type senderResponse struct {
	io.ReadCloser
	sender *requestSender
	once   sync.Once
	err    error
}

func (body *senderResponse) Close() error {
	body.once.Do(func() {
		rawErr := body.ReadCloser.Close()
		select {
		case <-body.sender.done:
		default:
			if body.sender.abortWrite != nil {
				body.sender.abortWrite()
			}
		}
		inputErr := body.sender.Close()
		<-body.sender.done
		body.err = errors.Join(rawErr, inputErr, body.sender.sendErr)
	})
	return body.err
}
