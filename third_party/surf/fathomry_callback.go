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
package surf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"time"

	utls "github.com/refraction-networking/utls"
)

// ErrFathomryCallback identifies a contained native callback panic.
var ErrFathomryCallback = errors.New("surf: native callback panic")

type fathomryCallbackError struct{ cause error }

func (*fathomryCallbackError) Error() string { return "surf: native callback panic" }
func (err *fathomryCallbackError) Unwrap() []error {
	if err.cause == nil {
		return []error{ErrFathomryCallback}
	}
	return []error{ErrFathomryCallback, err.cause}
}

func fathomryInvoke(callback func() error) (err error) {
	returned := false
	defer func() {
		if !returned {
			cause, _ := recover().(error)
			err = &fathomryCallbackError{cause: cause}
		}
	}()
	err = callback()
	returned = true
	return err
}

func (state *fathomryState) callback(callback func() error) error {
	finish, err := state.enter(state.ctx)
	if err != nil {
		return err
	}
	defer finish()
	return fathomryInvoke(callback)
}

func (state *fathomryState) notification(callback func() error) {
	finish, err := state.enter(state.ctx)
	if err != nil {
		return
	}
	defer finish()
	state.callbackFailed(fathomryInvoke(callback))
}

func (state *fathomryState) callbackFailed(err error) {
	if err == nil {
		return
	}
	state.mu.Lock()
	if state.failed == nil {
		state.failed = err
	}
	state.mu.Unlock()
	state.cancel()
}

func (state *fathomryState) failure() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.failed
}

type fathomryReader struct {
	state *fathomryState
	raw   io.Reader
}

func (reader fathomryReader) Read(data []byte) (count int, err error) {
	err = reader.state.callback(func() error { count, err = reader.raw.Read(data); return err })
	return count, err
}

type fathomryWriter struct {
	state *fathomryState
	raw   io.Writer
}

func (writer fathomryWriter) Write(data []byte) (count int, err error) {
	err = writer.state.callback(func() error { count, err = writer.raw.Write(data); return err })
	return count, err
}
func (state *fathomryState) clock(clock func() time.Time) func() time.Time {
	return func() (value time.Time) {
		state.notification(func() error { value = clock(); return nil })
		return value
	}
}
func (state *fathomryState) guardTLS(config *tls.Config) {
	if verify := config.VerifyPeerCertificate; verify != nil {
		config.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
			return state.callback(func() error { return verify(raw, chains) })
		}
	}
	if verify := config.VerifyConnection; verify != nil {
		config.VerifyConnection = func(value tls.ConnectionState) error {
			return state.callback(func() error { return verify(value) })
		}
	}
	if verify := config.EncryptedClientHelloRejectionVerify; verify != nil {
		config.EncryptedClientHelloRejectionVerify = func(value tls.ConnectionState) error {
			return state.callback(func() error { return verify(value) })
		}
	}
	if certificate := config.GetClientCertificate; certificate != nil {
		config.GetClientCertificate = func(info *tls.CertificateRequestInfo) (value *tls.Certificate, err error) {
			err = state.callback(func() error { value, err = certificate(info); return err })
			return value, err
		}
	}
	if config.Time != nil {
		config.Time = state.clock(config.Time)
	}
	if config.Rand != nil {
		config.Rand = fathomryReader{state, config.Rand}
	}
	if config.KeyLogWriter != nil {
		config.KeyLogWriter = fathomryWriter{state, config.KeyLogWriter}
	}
}
func (state *fathomryState) guardJA(config *utls.Config) {
	if verify := config.VerifyPeerCertificate; verify != nil {
		config.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
			return state.callback(func() error { return verify(raw, chains) })
		}
	}
	if verify := config.VerifyConnection; verify != nil {
		config.VerifyConnection = func(value utls.ConnectionState) error {
			return state.callback(func() error { return verify(value) })
		}
	}
	if verify := config.EncryptedClientHelloRejectionVerify; verify != nil {
		config.EncryptedClientHelloRejectionVerify = func(value utls.ConnectionState) error {
			return state.callback(func() error { return verify(value) })
		}
	}
	if certificate := config.GetClientCertificate; certificate != nil {
		config.GetClientCertificate = func(info *utls.CertificateRequestInfo) (value *utls.Certificate, err error) {
			err = state.callback(func() error { value, err = certificate(info); return err })
			return value, err
		}
	}
	if config.Time != nil {
		config.Time = state.clock(config.Time)
	}
	if config.Rand != nil {
		config.Rand = fathomryReader{state, config.Rand}
	}
	if config.KeyLogWriter != nil {
		config.KeyLogWriter = fathomryWriter{state, config.KeyLogWriter}
	}
}
