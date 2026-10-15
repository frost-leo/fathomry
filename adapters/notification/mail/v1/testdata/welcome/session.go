/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"context"
	"errors"
	"time"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

type mailSession struct {
	runtime     *adapters.Runtime
	inbox       *adapters.Inbox[mail.Result]
	owner       *mail.Owner
	summary     *submission
	sourceAcked bool
}

func openMail(ctx context.Context, settings mail.Settings, summary *submission) (session *mailSession, err error) {
	prepared, err := mail.Prepare(settings, mail.NativeOptions{})
	if err != nil {
		return nil, err
	}
	policy, err := prepared.Policy()
	if err != nil {
		return nil, err
	}
	session = &mailSession{summary: summary}
	session.runtime, err = adapters.New(ctx, policy.Runtime)
	if err != nil {
		return session, err
	}
	session.inbox, err = adapters.NewInbox[mail.Result](policy.Evidence)
	if err != nil {
		return session, err
	}
	session.owner, err = prepared.Open(ctx, mail.Dependencies{Runtime: session.runtime, Evidence: session.inbox})
	return session, err
}

func (session *mailSession) close() error {
	if session == nil {
		return nil
	}
	wait, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var joined error
	if session.owner != nil {
		joined = session.owner.Close(wait)
		session.summary.SourceReleased = session.owner.ShutdownComplete()
		session.summary.CleanupFailure = session.summary.CleanupFailure || joined != nil
	}
	if session.runtime != nil {
		err := session.runtime.Close(wait)
		session.summary.RuntimeClosed = err == nil
		joined = errors.Join(joined, err)
	}
	if session.inbox != nil {
		status, err := session.inbox.Inspect()
		if err == nil && !session.sourceAcked && status.Outstanding > 0 && (session.summary.SourceReleased || session.summary.RuntimeClosed) {
			_, sourceErr := session.take("open", mail.Result{})
			joined = errors.Join(joined, sourceErr)
			status, err = session.inbox.Inspect()
		}
		session.summary.EvidenceOutstanding = status.Outstanding
		joined = errors.Join(joined, err)
		if status.Outstanding != 0 {
			joined = errors.Join(joined, errors.New("welcome evidence custody remains outstanding"))
		}
	}
	return joined
}

func (session *mailSession) accept(operation string, returned mail.Result, operationErr error) (mail.Result, error) {
	status, err := session.inbox.Inspect()
	if err != nil {
		return mail.Result{}, errors.Join(operationErr, err)
	}
	sourceRecords := 1
	if session.sourceAcked {
		sourceRecords = 0
	}
	if status.Outstanding <= sourceRecords {
		session.summary.PrimaryFailure = session.summary.PrimaryFailure || operationErr != nil
		return returned, operationErr
	}
	recorded, err := session.take(operation, returned)
	return recorded, errors.Join(operationErr, err)
}

func (session *mailSession) take(operation string, returned mail.Result) (mail.Result, error) {
	wait, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sourceErr error
	for range 2 {
		entry, err := session.inbox.NextReleased(wait)
		if err != nil {
			return mail.Result{}, errors.Join(sourceErr, err)
		}
		defer entry.Retry()
		receipt, err := entry.Receipt()
		if err != nil {
			return mail.Result{}, errors.Join(sourceErr, err)
		}
		snapshot, err := receipt.WaitReleased(wait)
		if err != nil {
			return mail.Result{}, errors.Join(sourceErr, err)
		}
		if snapshot.Info().Operation == "notification.mail.open" {
			if session.sourceAcked {
				return mail.Result{}, errors.New("duplicate welcome source evidence")
			}
			if err := entry.Ack(); err != nil {
				return mail.Result{}, err
			}
			session.sourceAcked = true
			session.summary.EvidenceRecords++
			session.summary.PrimaryFailure = session.summary.PrimaryFailure || snapshot.Primary() != nil
			session.summary.CleanupFailure = session.summary.CleanupFailure || snapshot.Cleanup() != nil
			sourceErr = errors.Join(sourceErr, snapshot.Err())
			if operation == "open" {
				return mail.Result{}, sourceErr
			}
			continue
		}
		if snapshot.Info().Operation != "notification.mail."+operation {
			return mail.Result{}, errors.New("unexpected welcome evidence operation")
		}
		recorded, present := snapshot.ValueCopy()
		if returned.Attribution().Sequence != 0 && (!present || recorded.Attribution() != returned.Attribution() || recorded.Attempts() != returned.Attempts() || recorded.BytesWritten() != returned.BytesWritten()) {
			return mail.Result{}, errors.New("welcome receipt/evidence mismatch")
		}
		session.summary.PrimaryFailure = session.summary.PrimaryFailure || snapshot.Primary() != nil
		session.summary.CleanupFailure = session.summary.CleanupFailure || snapshot.Cleanup() != nil
		if err := entry.Ack(); err != nil {
			return mail.Result{}, err
		}
		session.summary.EvidenceRecords++
		return recorded, errors.Join(sourceErr, snapshot.Err())
	}
	return mail.Result{}, errors.New("finite welcome evidence missing")
}

func (session *mailSession) send(ctx context.Context, message mail.Message) error {
	result, operationErr := session.owner.Client().Send(ctx, message)
	recorded, err := session.accept("send", result, operationErr)
	deliveries := recorded.Messages()
	if len(deliveries) != 1 {
		return errors.Join(err, errors.New("message evidence missing"))
	}
	delivery := deliveries[0]
	session.summary.Observed = true
	session.summary.MessageID = delivery.ID()
	session.summary.Stage = delivery.Stage()
	session.summary.Code = delivery.Code()
	session.summary.EnhancedCode = delivery.EnhancedCode()
	session.summary.Attempts, session.summary.AttemptsExact = recorded.Attempts().Observed, recorded.Attempts().Exact
	switch delivery.Effect() {
	case mail.NotAttempted:
		session.summary.Effect = "not_attempted"
	case mail.NotAccepted:
		session.summary.Effect = "not_accepted"
	case mail.Unknown:
		session.summary.Effect = "unknown"
	case mail.Accepted:
		session.summary.Effect = "accepted"
	}
	if delivery.Effect() != mail.Accepted {
		err = errors.Join(err, errors.New("relay acceptance was not established"))
	}
	return err
}

func submit(ctx context.Context, settings mail.Settings, message mail.Message) (summary submission, err error) {
	session, err := openMail(ctx, settings, &summary)
	if session != nil {
		defer func() { err = errors.Join(err, session.close()) }()
	}
	if err != nil {
		return summary, err
	}
	return summary, session.send(ctx, message)
}
