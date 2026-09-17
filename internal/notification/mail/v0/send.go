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

package mail

import (
	"context"
	"errors"
	"io"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	smtp "github.com/wneessen/go-mail/smtp"
)

// Effect describes SMTP submission only, never inbox placement or human reading.
type Effect uint8

const (
	// NotAttempted means this message's MAIL command was not entered.
	NotAttempted Effect = iota
	// NotAccepted means DATA completion was not attempted or explicitly rejected.
	// Accepted RCPT commands alone do not imply submission.
	NotAccepted
	// Unknown means DATA bytes/completion may have reached the relay without a
	// trustworthy final acknowledgement. Automatic resending is never performed.
	Unknown
	// Accepted means the relay returned 250 after the DATA terminator.
	Accepted
)

// Stage identifies the last entered submission/composition step. Reset and
// connection cleanup errors are separate from the captured submission reply.
type Stage string

const (
	StageCompose   Stage = "compose"
	StageConnect   Stage = "connect"
	StageMail      Stage = "mail"
	StageRecipient Stage = "recipient"
	StageData      Stage = "data"
	StageBody      Stage = "body"
	StageAck       Stage = "ack"
)

// Recipient is an immutable RCPT-stage snapshot. The enclosing slice retains
// order: To, then Cc, then Bcc. Mailboxes are private data, not safe log labels.
// Accepted is only RCPT acceptance; Code zero means no exact code was captured.
type Recipient struct {
	private
	address             string
	attempted, accepted bool
	code                int
	enhanced            string
}

func (recipient Recipient) Address() string      { return recipient.address }
func (recipient Recipient) Attempted() bool      { return recipient.attempted }
func (recipient Recipient) Accepted() bool       { return recipient.accepted }
func (recipient Recipient) Code() int            { return recipient.code }
func (recipient Recipient) EnhancedCode() string { return recipient.enhanced }

// Delivery associates one immutable message identity, envelope sender, recipient
// stages and effect. Code/EnhancedCode concern the last reply captured at Stage,
// not recipient-wide status. Err preserves deliberate native cause inspection.
type Delivery struct {
	private
	id, sender string
	recipients []Recipient
	effect     Effect
	stage      Stage
	code       int
	enhanced   string
	err        error
}

func (delivery Delivery) ID() string     { return delivery.id }
func (delivery Delivery) Sender() string { return delivery.sender }
func (delivery Delivery) Recipients() []Recipient {
	return append([]Recipient(nil), delivery.recipients...)
}
func (delivery Delivery) Effect() Effect       { return delivery.effect }
func (delivery Delivery) Stage() Stage         { return delivery.stage }
func (delivery Delivery) Code() int            { return delivery.code }
func (delivery Delivery) EnhancedCode() string { return delivery.enhanced }
func (delivery Delivery) Err() error           { return delivery.err }

// Result is an immutable finite batch snapshot. Entries retain input order,
// including unsent suffixes after fail-fast termination. There is no batch-level
// atomicity or rollback. Native error causes are inspection-only borrowed values.
type Result struct {
	private
	deliveries []Delivery
}

func (result Result) Messages() []Delivery { return append([]Delivery(nil), result.deliveries...) }

// Send submits one immutable message. Setup/admission failures return an error;
// admitted outcomes (including validation, partial effect and cleanup failures)
// are obtained from the returned receipt and independently reserved inbox.
func (client *Client) Send(ctx context.Context, id fault.Correlation, message Message) (*invocation.Receipt[Result], error) {
	return client.SendBatch(ctx, id, []Message{message})
}

// SendBatch processes messages sequentially on one reusable connection. It stops
// at the first failure, never retries/reconnects/resends within a call, and retains
// every input position. The caller must not mutate the slice during this method.
func (client *Client) SendBatch(ctx context.Context, id fault.Correlation, messages []Message) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, "send")
	}
	value := client.owner.settings
	if err := value.check(messages); err != nil {
		return nil, err
	}
	call, err := invocation.Begin(ctx, client.access, invocation.Request{
		Name: "send", Correlation: id, Shape: invocation.Finite, Bytes: value.reservation(), EvidenceBytes: value.evidenceReservation(),
		Admission: invocation.Budget{Limit: value.Timeout}, AttemptsKnown: true, MaxAttempts: uint64(len(messages))}, client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	result := Result{deliveries: make([]Delivery, len(messages))}
	for index, message := range messages {
		delivery := Delivery{id: message.content.ID, sender: message.sender, recipients: make([]Recipient, len(message.recipients))}
		for pos, addr := range message.recipients {
			delivery.recipients[pos].address = addr
		}
		result.deliveries[index] = delivery
	}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: err})
		return call.Receipt(), nil
	}
	defer cancel()
	var conn *connection
	stop := func() {}
	var primary, cleanup error
	for index, message := range messages {
		delivery := &result.deliveries[index]
		delivery.stage = StageCompose
		encoded, composeErr := compose(work, message, value)
		if composeErr != nil {
			primary = composeErr
			delivery.err = phaseFailure(ErrSend, "compose", work, primary)
			break
		}
		if work.Err() != nil {
			primary = work.Err()
			delivery.err = phaseFailure(ErrSend, "compose", work, primary)
			break
		}
		if conn == nil {
			delivery.stage = StageConnect
			conn, stop, err = client.owner.acquire(work)
			if err != nil {
				primary = err
				delivery.err = phaseFailure(ErrSend, "connect", work, err)
				delivery.code, delivery.enhanced = replyError(err)
				break
			}
		}
		if work.Err() != nil {
			primary = work.Err()
			delivery.err = phaseFailure(ErrSend, "send", work, primary)
			break
		}
		_, _ = call.Attempt()
		primary, cleanup = sendMessage(work, conn, encoded, delivery)
		if primary != nil || cleanup != nil {
			break
		}
	}
	stop()
	if conn != nil {
		if primary == nil && cleanup == nil && work.Err() == nil {
			client.owner.put(conn)
		} else if closeErr := conn.raw.Close(); closeErr != nil {
			cleanup = errors.Join(cleanup, closeErr)
		}
	}
	if primary != nil {
		primary = phaseFailure(ErrSend, "send", work, primary)
	}
	if cleanup != nil {
		cleanup = phaseFailure(ErrCleanup, "reset", work, cleanup)
	}
	call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: primary, Cleanup: cleanup})
	return call.Receipt(), nil
}
func sendMessage(ctx context.Context, conn *connection, data []byte, delivery *Delivery) (primary, cleanup error) {
	defer func() {
		if primary != nil {
			delivery.err = phaseFailure(ErrSend, string(delivery.stage), ctx, primary)
			delivery.code, delivery.enhanced = replyError(primary)
		}
		if cleanup != nil {
			delivery.err = phaseFailure(ErrCleanup, "reset", ctx, cleanup)
		}
	}()
	delivery.stage = StageMail
	delivery.effect = NotAccepted
	if err := conn.native.Mail("<" + delivery.sender + ">"); err != nil {
		return err, nil
	}
	for index := range delivery.recipients {
		delivery.stage = StageRecipient
		recipient := &delivery.recipients[index]
		recipient.attempted = true
		err := conn.native.Rcpt("<" + recipient.address + ">")
		recipient.code, recipient.enhanced = replyError(err)
		if err != nil {
			return err, nil
		}
		recipient.accepted = true
	}
	delivery.stage = StageData
	writer, err := conn.native.Data()
	if err != nil {
		return err, nil
	}
	delivery.stage = StageBody
	delivery.effect = Unknown
	if count, err := writer.Write(data); err != nil {
		return err, nil
	} else if count != len(data) {
		return io.ErrShortWrite, nil
	}
	delivery.stage = StageAck
	code, response, err := finishData(writer, conn.native)
	if err != nil {
		status, _ := replyError(err)
		if status >= 400 && status < 600 {
			delivery.effect = NotAccepted
		}
		return err, nil
	}
	delivery.effect = Accepted
	delivery.code = code
	delivery.enhanced = enhancedCode(response)
	// Cleanup failure cannot undo the preceding DATA acknowledgement.
	if err = conn.native.Reset(); err != nil {
		return nil, err
	}
	return nil, nil
}

func finishData(writer io.WriteCloser, native *smtp.Client) (int, string, error) {
	// go-mail v0.8.1 DataCloser.Close discards the DotWriter.Close error. Only
	// read the acknowledgement after a successful terminator flush.
	closer, ok := writer.(*smtp.DataCloser)
	if !ok {
		return 0, "", failure(ErrUnsupported, "data-writer")
	}
	if err := closer.WriteCloser.Close(); err != nil {
		return 0, "", err
	}
	return native.Text.ReadResponse(250)
}
func replyError(err error) (int, string) {
	var reply *textproto.Error
	if errors.As(err, &reply) {
		return reply.Code, enhancedCode(reply.Msg)
	}
	return 0, ""
}
func enhancedCode(text string) string {
	token, _, _ := strings.Cut(text, " ")
	if len(token) < 5 || len(token) > 9 {
		return ""
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(parts[0]) != 1 || !strings.Contains("245", parts[0]) {
		return ""
	}
	for _, part := range parts[1:] {
		if len(part) < 1 || len(part) > 3 {
			return ""
		}
		if _, err := strconv.ParseUint(part, 10, 10); err != nil {
			return ""
		}
	}
	return token
}
