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
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	mail "github.com/frost-leo/fathomry/internal/notification/mail/v0"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.yaml.in/yaml/v3"
)

type smtpSettings struct {
	Enabled  bool   `yaml:"enabled"`
	Host     string `yaml:"smtp_host"`
	Port     int    `yaml:"smtp_port"`
	TLSMode  string `yaml:"tls_mode"`
	Auth     string `yaml:"auth"`
	Hello    string `yaml:"hello"`
	Username string `yaml:"smtp_username"`
	Password string `yaml:"smtp_password"`
	From     string `yaml:"from"`
	To       string `yaml:"to"`
}
type submission struct {
	Observed       bool       `json:"observed"`
	MessageID      string     `json:"message_id,omitempty"`
	Effect         string     `json:"effect,omitempty"`
	Stage          mail.Stage `json:"stage,omitempty"`
	Code           int        `json:"code,omitempty"`
	EnhancedCode   string     `json:"enhanced_code,omitempty"`
	PrimaryFailure bool       `json:"primary_failure"`
	CleanupFailure bool       `json:"cleanup_failure"`
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
	send := flags.Bool("send", false, "send exactly one owner-authorized test message")
	if err := flags.Parse(args); err != nil {
		return errors.New("invalid welcome command arguments")
	}
	if flags.NArg() != 0 || (*send) == (*previewPath != "") || *send && *configPath == "" || !*send && *configPath != "" {
		return errors.New("choose either -preview FILE or -send -config FILE")
	}
	from, to := "sender@fixture.test", "recipient@fixture.test"
	var settings smtpSettings
	if *send {
		var err error
		settings, err = readSettings(*configPath)
		if err != nil {
			return err
		}
		if !settings.Enabled {
			return errors.New("sending is disabled")
		}
		from, to = settings.From, settings.To
	}
	id := "gh63-" + time.Now().UTC().Format("20060102T150405.000000000") + "@fathomry.test"
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
	message, err := mail.NewMessage(content)
	if err != nil {
		return err
	}
	if !*send {
		return writePreview(*previewPath, content)
	}
	options := mail.OptionsV1{Name: "welcome-example", Host: settings.Host, Port: settings.Port, Hello: settings.Hello,
		TLSMode: settings.TLSMode, Auth: settings.Auth, Username: settings.Username, Password: settings.Password,
		MaxActive: 1, MaxMessages: 1, MaxRecipients: 1, Timeout: 30 * time.Second, CloseTimeout: 3 * time.Second}
	summary, sendErr := submit(ctx, options, message)
	encodeErr := json.NewEncoder(output).Encode(summary)
	return errors.Join(sendErr, encodeErr)
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
func submit(ctx context.Context, options mail.OptionsV1, message mail.Message) (summary submission, err error) {
	selected, err := mail.Select(options)
	if err != nil {
		return summary, err
	}
	selected = resource.WithLimits(selected, mail.LimitsV1(options))
	initializationCleanup, endInitialization := context.WithTimeout(context.Background(), 5*time.Second)
	assembly, err := resource.Assemble(ctx, initializationCleanup, "welcome-example", selected)
	endInitialization()
	if assembly != nil {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			closeErr := assembly.Close(cleanup)
			summary.CleanupFailure = summary.CleanupFailure || closeErr != nil
			err = errors.Join(err, closeErr)
		}()
	}
	if err != nil {
		return summary, err
	}
	inbox, err := invocation.NewInbox[mail.Result](1, 8<<20)
	if err != nil {
		return summary, err
	}
	client, err := mail.Bind(assembly, selected, inbox, nil)
	if err != nil {
		return summary, err
	}
	_, err = client.Send(ctx, fault.Correlation{Call: "welcome"}, message)
	if err != nil {
		summary.PrimaryFailure = true
		return summary, err
	}
	// The command owns evidence reception independently of the send context.
	evidence, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record, err := inbox.Next(evidence)
	if err != nil {
		return summary, err
	}
	result, err := record.Receipt().WaitFinal(evidence)
	if err != nil {
		return summary, err
	}
	err = errors.Join(result.Err(), record.Release())
	deliveries := result.Outcome.Value.Messages()
	if len(deliveries) != 1 {
		return summary, errors.Join(err, errors.New("message evidence missing"))
	}
	delivery := deliveries[0]
	summary.Observed = true
	summary.MessageID = delivery.ID()
	summary.Stage = delivery.Stage()
	summary.Code = delivery.Code()
	summary.EnhancedCode = delivery.EnhancedCode()
	summary.PrimaryFailure = result.Outcome.Primary != nil
	summary.CleanupFailure = result.Outcome.Cleanup != nil
	switch delivery.Effect() {
	case mail.NotAttempted:
		summary.Effect = "not_attempted"
	case mail.NotAccepted:
		summary.Effect = "not_accepted"
	case mail.Unknown:
		summary.Effect = "unknown"
	case mail.Accepted:
		summary.Effect = "accepted"
	}
	if delivery.Effect() != mail.Accepted {
		err = errors.Join(err, errors.New("relay acceptance was not established"))
	}
	return summary, err
}
func writePreview(path string, content mail.Content) error {
	html := content.HTML
	for _, asset := range content.Inline {
		html = strings.ReplaceAll(html, "cid:"+asset.ID, "data:"+asset.ContentType+";base64,"+base64.StdEncoding.EncodeToString(asset.Data))
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("preview output must be a new writable file")
	}
	_, writeErr := io.WriteString(file, html)
	return errors.Join(writeErr, file.Close())
}
