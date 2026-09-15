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

package httpcloak

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/sardanioss/httpcloak/transport"
	"io"
)

func (current *binding) enterCallback() (func(error), error) {
	current.notifyMu.Lock()
	defer current.notifyMu.Unlock()
	if current.notifyClosed || current.callbackCount >= current.callbackLimit {
		return nil, failure(ErrLimit, "native-callbacks")
	}
	current.callbackCount++
	current.callbackActive++
	return func(err error) {
		current.notifyMu.Lock()
		defer current.notifyMu.Unlock()
		if err != nil {
			current.notifications = append(current.notifications, err)
		}
		current.callbackActive--
		if current.notifyClosed && current.callbackActive == 0 {
			close(current.notifyDone)
		}
	}, nil
}
func (current *binding) stopCallbacks() <-chan struct{} {
	current.notifyMu.Lock()
	defer current.notifyMu.Unlock()
	if !current.notifyClosed {
		current.notifyClosed = true
		if current.callbackActive == 0 {
			close(current.notifyDone)
		}
	}
	return current.notifyDone
}
func (current *binding) takeNotifications() []error {
	current.notifyMu.Lock()
	defer current.notifyMu.Unlock()
	result := current.notifications
	current.notifications = nil
	current.callbackCount = 0
	return result
}
func (current *binding) verification(original *transport.TLSVerify) *transport.TLSVerify {
	if original == nil {
		return nil
	}
	value := *original
	if original.VerifyPeerCertificate != nil {
		value.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
			done, err := current.enterCallback()
			if err != nil {
				return err
			}
			err = original.VerifyPeerCertificate(raw, chains)
			done(err)
			return err
		}
	}
	if original.VerifyConnection != nil {
		value.VerifyConnection = func(state tls.ConnectionState) error {
			done, err := current.enterCallback()
			if err != nil {
				return err
			}
			err = original.VerifyConnection(state)
			done(err)
			return err
		}
	}
	return &value
}

type keyLogWriter struct {
	binding *binding
	writer  io.Writer
}

func (writer keyLogWriter) Write(data []byte) (int, error) {
	done, err := writer.binding.enterCallback()
	if err != nil {
		return 0, err
	}
	count, err := writer.writer.Write(data)
	done(err)
	return count, err
}
func (op *operation) recordNotifications(current *binding) {
	notes := current.takeNotifications()
	if len(notes) == 0 {
		return
	}
	op.mu.Lock()
	op.data.notifications = append(op.data.notifications, notes...)
	op.mu.Unlock()
	op.fail(failure(ErrTransport, "native-callback", errors.Join(notes...)))
}
