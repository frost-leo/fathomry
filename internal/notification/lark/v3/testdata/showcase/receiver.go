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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	"github.com/frost-leo/fathomry/internal/resource"
)

type receiveEvidence struct {
	Phase       string `json:"phase"`
	Connected   bool   `json:"connected"`
	Connections uint64 `json:"connections"`
	Pongs       uint64 `json:"pongs"`
	Delivered   uint64 `json:"delivered"`
	AckWritten  uint64 `json:"ack_written"`
	AckUnknown  uint64 `json:"ack_unknown"`
	Saved       bool   `json:"saved"`
	Failed      bool   `json:"failed"`
	Selection   string `json:"selection,omitempty"`
}
type listenerDone struct {
	receipt *invocation.Receipt[lark.WebSocketResult]
	err     error
}

func listen(ctx context.Context, options lark.OptionsV1, duration time.Duration, email, text, eventPath string, output io.Writer) (resultErr error) {
	if options.WebSocket == nil {
		options.WebSocket = &lark.WebSocketOptions{}
	}
	selected, err := lark.Select(options)
	if err != nil {
		return err
	}
	selected = resource.WithLimits(selected, lark.LimitsV1(options))
	assembly, err := resource.Assemble(ctx, ctx, "receiver-example", selected)
	if err != nil {
		return err
	}
	var receiver *lark.Receiver
	connected, saved := false, false
	defer func() {
		if receiver == nil {
			return
		}
		status := receiver.Status()
		connected = connected || status.Connections > 0
		if !connected || status.Pongs == 0 {
			resultErr = errors.Join(resultErr, errors.New("WebSocket did not establish a verified heartbeat exchange"))
		}
		if text != "" && !saved {
			resultErr = errors.Join(resultErr, errors.New("authorized probe event was not observed"))
		}
		reportErr := json.NewEncoder(output).Encode(receiveEvidence{Phase: "summary", Connected: connected, Connections: status.Connections, Pongs: status.Pongs,
			Delivered: status.Delivered, AckWritten: status.AckWritten, AckUnknown: status.AckUnknown, Saved: saved, Failed: resultErr != nil})
		resultErr = errors.Join(resultErr, reportErr)
	}()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, assembly.Close(cleanup))
	}()
	recipientID := ""
	if email != "" {
		inbox, _ := invocation.NewInbox[lark.Result](1, lark.EvidenceBytesV1(options))
		client, err := lark.Bind(assembly, selected, inbox, nil)
		if err != nil {
			return err
		}
		session := &session{client, inbox, output}
		receipt, err := client.ResolveEmail(ctx, callID(), email)
		result, err := session.await(ctx, receipt, err)
		if err != nil {
			return err
		}
		recipient, ok := result.ResolvedRecipient()
		if !ok {
			return errUnresolvedRecipient
		}
		recipientID = recipient.ID
	}
	results, _ := invocation.NewInbox[lark.WebSocketResult](32, 32*lark.WebSocketEvidenceBytes())
	events, _ := invocation.NewInbox[lark.WebSocketEvent](16, 16*lark.WebSocketEventBytesV1(options))
	receiver, err = lark.BindReceiver(assembly, selected, results, nil)
	if err != nil {
		return err
	}
	work, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	finished := make(chan listenerDone, 1)
	go func() {
		receipt, err := receiver.Listen(work, callID(), events)
		finished <- listenerDone{receipt, err}
	}()
	held := []*invocation.DeliveryRecord[lark.WebSocketResult]{}
	sweep := func() error {
		handling, end := context.WithTimeout(context.Background(), time.Second)
		defer end()
		for results.Usage().Outstanding > len(held) {
			record, err := results.Next(handling)
			if err != nil {
				return err
			}
			held = append(held, record)
		}
		kept := held[:0]
		for _, record := range held {
			result, present := record.Receipt().Result()
			if !present || !result.Final || !result.Released {
				kept = append(kept, record)
				continue
			}
			if err := record.Release(); err != nil {
				return err
			}
		}
		held = kept
		return nil
	}
	var final listenerDone
	ended := false
	defer func() {
		cancel()
		if !ended {
			final = <-finished
		}
		handling, end := context.WithTimeout(context.Background(), 3*time.Second)
		defer end()
		if final.err != nil {
			resultErr = errors.Join(resultErr, final.err)
		} else if final.receipt != nil {
			result, err := final.receipt.WaitReleased(handling)
			resultErr = errors.Join(resultErr, err, result.Outcome.Cleanup)
			if primary := result.Outcome.Primary; primary != nil && !errors.Is(primary, context.Canceled) && !errors.Is(primary, context.DeadlineExceeded) {
				resultErr = errors.Join(resultErr, primary)
			}
		}
		for events.Usage().Outstanding > 0 {
			record, err := events.Next(handling)
			if err != nil {
				resultErr = errors.Join(resultErr, err)
				break
			}
			_, err = record.Receipt().WaitReleased(handling)
			resultErr = errors.Join(resultErr, err, record.Release())
		}
		resultErr = errors.Join(resultErr, sweep())
	}()
	rejectedText := 0
	for {
		status := receiver.Status()
		if status.Connected && !connected {
			connected = true
			if err := json.NewEncoder(output).Encode(receiveEvidence{Phase: "connected", Connected: true, Connections: status.Connections}); err != nil {
				return err
			}
		}
		if err := sweep(); err != nil {
			return err
		}
		select {
		case final = <-finished:
			ended = true
			goto complete
		default:
		}
		if work.Err() != nil {
			goto complete
		}
		poll, end := context.WithTimeout(work, 100*time.Millisecond)
		record, err := events.Next(poll)
		end()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				continue
			}
			return err
		}
		handling, done := context.WithTimeout(work, time.Second)
		result, err := record.Receipt().WaitReleased(handling)
		done()
		if err != nil {
			return err
		}
		incoming := result.Outcome.Value
		selection := probeSelection(incoming.Event(), recipientID, text)
		status = receiver.Status()
		if err := json.NewEncoder(output).Encode(receiveEvidence{Phase: "event", Connected: status.Connected, Connections: status.Connections, Pongs: status.Pongs, Delivered: status.Delivered, AckWritten: status.AckWritten, AckUnknown: status.AckUnknown, Selection: selection}); err != nil {
			_ = record.Release()
			return err
		}
		if selection != "matched" {
			if selection == "different-text" && rejectedText < 8 {
				rejectedText++
				receipt, rejectErr := incoming.Reject(work)
				if rejectErr == nil {
					result, waitErr := receipt.WaitReleased(work)
					rejectErr = errors.Join(waitErr, result.Err())
				}
				releaseErr := record.Release()
				if rejectErr != nil || releaseErr != nil {
					return errors.Join(rejectErr, releaseErr)
				}
				continue
			}
			_ = record.Release()
			return errors.New("unexpected event left unacknowledged; probe stopped")
		}
		if err := saveProbe(eventPath, incoming.Event()); err != nil {
			_ = record.Release()
			return err
		}
		saved = true
		receipt, err := incoming.Acknowledge(work, lark.JSON{})
		if err == nil {
			ack, waitErr := receipt.WaitReleased(work)
			err = errors.Join(waitErr, ack.Err())
		}
		releaseErr := record.Release()
		if err != nil || releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		cancel()
		goto complete
	}
complete:
	return nil
}
func probeSelection(event lark.Event, sender, text string) string {
	if event.Type() != "im.message.receive_v1" || sender == "" {
		return "other-event"
	}
	fields, err := requiredJSONFields(event.JSONData(), "sender", "message")
	if err != nil {
		return "invalid-message"
	}
	senderFields, err := requiredJSONFields(fields["sender"], "sender_id")
	if err != nil {
		return "invalid-message"
	}
	if _, err := requiredJSONFields(senderFields["sender_id"], "open_id"); err != nil {
		return "invalid-message"
	}
	if _, err := requiredJSONFields(fields["message"], "message_type", "content"); err != nil {
		return "invalid-message"
	}
	var data struct {
		Sender struct {
			ID struct {
				OpenID string `json:"open_id"`
			} `json:"sender_id"`
		} `json:"sender"`
		Message struct {
			Type    string `json:"message_type"`
			Content string `json:"content"`
		} `json:"message"`
	}
	if json.Unmarshal(event.JSONData(), &data) != nil {
		return "invalid-message"
	}
	if data.Sender.ID.OpenID != sender {
		return "different-sender"
	}
	if data.Message.Type != "text" {
		return "not-text"
	}
	if _, err := lark.NewJSON([]byte(data.Message.Content)); err != nil {
		return "invalid-content"
	}
	if _, err := requiredJSONFields([]byte(data.Message.Content), "text"); err != nil {
		return "invalid-content"
	}
	var content struct {
		Text string `json:"text"`
	}
	if json.Unmarshal([]byte(data.Message.Content), &content) != nil {
		return "invalid-content"
	}
	if text == "" || content.Text != text {
		return "different-text"
	}
	return "matched"
}
func saveProbe(path string, event lark.Event) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("new owner-only event file unavailable")
	}
	value := struct {
		Schema, EventID, Type string
		Data                  json.RawMessage
	}{"feishu-example-event/v1", event.ID(), event.Type(), event.JSONData()}
	encodeErr := json.NewEncoder(file).Encode(value)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err = errors.Join(encodeErr, syncErr, closeErr); err != nil {
		return errors.New("event persistence failed")
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("event directory unavailable")
	}
	err = errors.Join(directory.Sync(), directory.Close())
	if err != nil {
		return errors.New("event directory sync failed")
	}
	return nil
}
