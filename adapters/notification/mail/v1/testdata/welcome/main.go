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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
	"go.yaml.in/yaml/v3"
)

type smtpSettings struct {
	Enabled   bool   `yaml:"enabled"`
	Host      string `yaml:"smtp_host"`
	Port      int    `yaml:"smtp_port"`
	TLSMode   string `yaml:"tls_mode"`
	RootCAPEM string `yaml:"root_ca_pem"`
	Auth      string `yaml:"auth"`
	Hello     string `yaml:"hello"`
	Username  string `yaml:"smtp_username"`
	Password  string `yaml:"smtp_password"`
	From      string `yaml:"from"`
	To        string `yaml:"to"`
}
type submission struct {
	Observed            bool       `json:"observed"`
	Prepared            bool       `json:"prepared"`
	MessageID           string     `json:"message_id,omitempty"`
	MIMEBytes           int        `json:"mime_bytes"`
	Effect              string     `json:"effect,omitempty"`
	Stage               mail.Stage `json:"stage,omitempty"`
	Code                int        `json:"code,omitempty"`
	EnhancedCode        string     `json:"enhanced_code,omitempty"`
	Attempts            uint64     `json:"attempts"`
	AttemptsExact       bool       `json:"attempts_exact"`
	PrimaryFailure      bool       `json:"primary_failure"`
	CleanupFailure      bool       `json:"cleanup_failure"`
	SourceReleased      bool       `json:"source_released"`
	RuntimeClosed       bool       `json:"runtime_closed"`
	EvidenceRecords     int        `json:"evidence_records"`
	EvidenceOutstanding int        `json:"evidence_outstanding"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "Welcome example failed. Inspect the bounded submission evidence; do not retry an unknown effect blindly.")
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("welcome", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "explicit private SMTP configuration")
	reportDirectory := flags.String("report", "", "captured repository report directory")
	previewPath := flags.String("preview", "", "new HTML preview path; sends nothing")
	attemptPath := flags.String("attempt", "", "new exclusive attempt record required for sending")
	send := flags.Bool("send", false, "send exactly one separately authorized test message")
	if err := flags.Parse(args); err != nil {
		return errors.New("invalid welcome command arguments")
	}
	if flags.NArg() != 0 || *send == (*previewPath != "") || *send && (*configPath == "" || *attemptPath == "") || !*send && (*configPath != "" || *attemptPath != "") {
		return errors.New("choose either -preview FILE or -send -config FILE -attempt FILE")
	}
	from, to := "sender@fixture.test", "recipient@fixture.test"
	settings := mail.Settings{Name: "welcome-example", Host: "localhost", Port: 465, Hello: "welcome.fixture.test", TLSMode: "implicit", Auth: "none", MaxActive: 1, MaxMessages: 1, MaxRecipients: 1, Timeout: 30 * time.Second, CloseTimeout: 3 * time.Second}
	if *send {
		private, err := readSettings(*configPath)
		if err != nil {
			return err
		}
		if !private.Enabled {
			return errors.New("sending is disabled")
		}
		from, to = private.From, private.To
		settings.Host, settings.Port, settings.Hello = private.Host, private.Port, private.Hello
		settings.TLSMode, settings.RootCAPEM = private.TLSMode, private.RootCAPEM
		settings.Auth, settings.Username, settings.Password = private.Auth, private.Username, private.Password
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return errors.New("welcome message identity unavailable")
	}
	id := "welcome-" + hex.EncodeToString(random) + "@fathomry.test"
	content, err := welcomeContent(from, to, id)
	if err != nil {
		return err
	}
	if *reportDirectory != "" {
		content, err = loadRepositoryContent(content, *reportDirectory, *send)
		if err != nil {
			return err
		}
	}
	summary, operationErr := executeWelcome(ctx, settings, content, *previewPath, *attemptPath)
	return errors.Join(operationErr, json.NewEncoder(output).Encode(summary))
}

func readSettings(path string) (smtpSettings, error) {
	var root struct {
		Mail smtpSettings `yaml:"fathomry_mail_gh63"`
	}
	file, err := os.Open(path)
	if err != nil {
		return root.Mail, errors.New("private configuration cannot be opened")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		return root.Mail, errors.New("private configuration must be an owner-only bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 || yaml.Unmarshal(data, &root) != nil {
		return root.Mail, errors.New("private configuration is invalid")
	}
	return root.Mail, nil
}

func executeWelcome(ctx context.Context, settings mail.Settings, content mail.Content, previewPath, attemptPath string) (summary submission, err error) {
	session, err := openMail(ctx, settings, &summary)
	var attempt *os.File
	defer func() {
		err = errors.Join(err, session.close())
		if attempt != nil {
			err = errors.Join(err, writeAttempt(attempt, "finished", summary), attempt.Close())
		}
	}()
	if err != nil {
		return summary, err
	}
	input, plan, err := prepareWelcome(content)
	if err != nil {
		summary.PrimaryFailure = true
		return summary, err
	}
	result, operationErr := session.owner.Client().PrepareMessage(ctx, input, plan)
	result, err = session.accept("prepare-message", result, operationErr)
	if err != nil {
		return summary, err
	}
	message, present := result.PreparedMessage()
	if !present {
		return summary, errors.New("prepared welcome message missing")
	}
	summary.Prepared = true
	summary.MessageID = message.ContentCopy().ID
	var wire bytes.Buffer
	result, operationErr = session.owner.Client().Export(ctx, message, &wire)
	result, err = session.accept("export", result, operationErr)
	if err != nil {
		return summary, err
	}
	if result.BytesWritten() != int64(wire.Len()) || !bytes.Equal(wire.Bytes(), message.EMLCopy()) {
		return summary, errors.New("welcome MIME export mismatch")
	}
	summary.MIMEBytes = wire.Len()
	if previewPath != "" {
		return summary, writePreview(previewPath, message)
	}
	if attemptPath == "" {
		return summary, errors.New("sending requires an explicit attempt record")
	}
	attempt, err = os.OpenFile(attemptPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return summary, errors.New("attempt record exists or cannot be created; sending refused")
	}
	if err := writeAttempt(attempt, "reserved", summary); err != nil {
		return summary, err
	}
	return summary, session.send(ctx, message)
}
func writeAttempt(file *os.File, event string, summary submission) error {
	value := struct {
		Event   string     `json:"event"`
		At      time.Time  `json:"at"`
		Outcome submission `json:"outcome"`
	}{event, time.Now().UTC(), summary}
	if json.NewEncoder(file).Encode(value) != nil || file.Sync() != nil {
		return errors.New("cannot persist welcome attempt evidence")
	}
	return nil
}
func inlineDataURI(asset mail.Inline) string {
	return "data:" + asset.ContentType + ";base64," + base64.StdEncoding.EncodeToString(asset.Data)
}
func writePreview(path string, message mail.Message) error {
	html, err := previewHTML(message)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("preview output must be a new writable file")
	}
	_, writeErr := io.WriteString(file, html)
	return errors.Join(writeErr, file.Close())
}
