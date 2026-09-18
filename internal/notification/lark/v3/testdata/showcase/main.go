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
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/google/uuid"
	"go.yaml.in/yaml/v3"
)

var errUnresolvedRecipient = errors.New("authorized email did not resolve to an application-visible recipient")

func main() {
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 5*time.Minute)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "Feishu showcase failed. Inspect bounded evidence; do not blindly retry unknown effects.")
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("showcase", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	config := flags.String("config", "", "explicit owner-only YAML file with feishuid and feishusecret")
	recipient := flags.String("to", "", "explicit owner-authorized email recipient")
	recipientType := flags.String("to-type", "email", "email, open_id, union_id, user_id or chat_id")
	preview := flags.String("preview", "", "new local output directory; no network")
	send := flags.Bool("send", false, "send a Welcome card; optional repository report and CSV")
	reportPath := flags.String("report", "", "explicit repository snapshot.json from the mail example's read-only collector")
	exercise := flags.Bool("exercise", false, "exercise additional APIs and recall every created message")
	receiving := flags.Bool("listen", false, "bounded WebSocket readiness or exact-message probe; no subscription changes")
	duration := flags.Duration("duration", 30*time.Second, "WebSocket probe duration")
	acceptText := flags.String("accept-text", "", "exact synthetic text accepted only from -to email")
	eventFile := flags.String("event-file", "", "new owner-only event file, synced before acknowledging a matching probe")
	if err := flags.Parse(args); err != nil {
		return errors.New("invalid arguments")
	}
	modes := 0
	for _, enabled := range []bool{*send, *exercise, *receiving, *preview != ""} {
		if enabled {
			modes++
		}
	}
	if flags.NArg() != 0 || modes != 1 || (*send || *exercise) && (*config == "" || *recipient == "") || *receiving && (*config == "" || *duration < time.Second || *duration > 4*time.Minute) {
		return errors.New("choose preview, send or exercise with explicit config and recipient")
	}
	if *reportPath != "" && !*send && *preview == "" {
		return errors.New("repository reports require welcome preview or send mode")
	}
	if *preview != "" {
		if *config != "" || *recipient != "" || *recipientType != "email" {
			return errors.New("preview does not accept private inputs")
		}
		return writePreview(*preview, *reportPath)
	}
	options, err := readSettings(*config)
	if err != nil {
		return err
	}
	if *receiving {
		if (*acceptText == "") != (*eventFile == "") || *acceptText != "" && (*recipient == "" || *recipientType != "email") {
			return errors.New("message probe requires an email, exact text and new event file")
		}
		return listen(ctx, options, *duration, *recipient, *acceptText, *eventFile, output)
	}
	var payload *welcomePayload
	if *send {
		payload, err = welcome(*reportPath, true)
		if err != nil {
			return err
		}
	}
	return deliver(ctx, options, lark.Recipient{Type: *recipientType, ID: *recipient}, *exercise, payload, output)
}
func readSettings(path string) (lark.OptionsV1, error) {
	var root struct {
		AppID     string `yaml:"feishuid"`
		AppSecret string `yaml:"feishusecret"`
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return lark.OptionsV1{}, errors.New("private configuration unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		return lark.OptionsV1{}, errors.New("configuration must be an owner-only regular file at most 1 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil || len(data) > 1<<20 {
		return lark.OptionsV1{}, errors.New("configuration read failed")
	}
	if yaml.Unmarshal(data, &root) != nil || root.AppID == "" || root.AppSecret == "" {
		return lark.OptionsV1{}, errors.New("missing Feishu credentials")
	}
	return lark.OptionsV1{Name: "feishu-showcase", Profile: "application", AppID: root.AppID, AppSecret: root.AppSecret, MaxActive: 1, Timeout: 30 * time.Second}, nil
}
func writePreview(directory, reportPath string) error {
	payload, err := welcome(reportPath, false)
	if err != nil {
		return err
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	files := map[string][]byte{"card.json": payload.card.JSON().Bytes()}
	if payload.table != nil {
		files["repository-snapshot.csv"] = payload.table
	}
	for name, data := range files {
		path := filepath.Join(directory, name)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(data)
		if err := errors.Join(writeErr, file.Close()); err != nil {
			return err
		}
	}
	return nil
}

type evidence struct {
	Operation        string      `json:"operation"`
	Effect           lark.Effect `json:"effect"`
	HTTP             int         `json:"http_status"`
	Code             int         `json:"api_code"`
	CodeKnown        bool        `json:"api_code_known"`
	Attempts         uint64      `json:"http_attempts"`
	MessageIDPresent bool        `json:"message_id_present"`
	PrimaryFailure   bool        `json:"primary_failure"`
	CleanupFailure   bool        `json:"cleanup_failure"`
}
type session struct {
	client *lark.Client
	inbox  *invocation.Inbox[lark.Result]
	output io.Writer
}

func (session *session) await(ctx context.Context, receipt *invocation.Receipt[lark.Result], err error) (lark.Result, error) {
	if err != nil {
		return lark.Result{}, err
	}
	// Evidence handling is owned independently of an expired network-call context.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	direct, err := receipt.WaitReleased(ctx)
	if err != nil {
		return lark.Result{}, err
	}
	record, err := session.inbox.Next(ctx)
	if err != nil {
		return lark.Result{}, err
	}
	independent, err := record.Receipt().WaitReleased(ctx)
	if err != nil {
		return lark.Result{}, err
	}
	if direct.Context != independent.Context {
		return lark.Result{}, errors.New("evidence association changed")
	}
	value := independent.Outcome.Value
	code, known := value.Exchange().APICode()
	status := value.Exchange().HTTPStatus()
	if auth, attempted := value.Authentication(); attempted && status == 0 {
		code, known = auth.APICode()
		status = auth.HTTPStatus()
	}
	reportErr := json.NewEncoder(session.output).Encode(evidence{Operation: independent.Context.Operation, Effect: value.Effect(),
		HTTP: status, Code: code, CodeKnown: known, Attempts: independent.Attempts.Observed, MessageIDPresent: value.MessageID() != "",
		PrimaryFailure: independent.Outcome.Primary != nil, CleanupFailure: independent.Outcome.Cleanup != nil})
	releaseErr := record.Release()
	return value, errors.Join(independent.Err(), reportErr, releaseErr)
}
func callID() fault.Correlation { return fault.Correlation{Call: "gh64-" + uuid.NewString()} }
func deliver(ctx context.Context, options lark.OptionsV1, to lark.Recipient, exercise bool, welcome *welcomePayload, output io.Writer) (resultErr error) {
	if welcome != nil && exercise {
		return errors.New("welcome sending and synthetic lifecycle exercise are distinct modes")
	}
	selected, err := lark.Select(options)
	if err != nil {
		return err
	}
	selected = resource.WithLimits(selected, lark.LimitsV1(options))
	assembly, err := resource.Assemble(ctx, ctx, "showcase", selected)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, assembly.Close(cleanup))
	}()
	inbox, err := invocation.NewInbox[lark.Result](1, lark.EvidenceBytesV1(options))
	if err != nil {
		return err
	}
	client, err := lark.Bind(assembly, selected, inbox, nil)
	if err != nil {
		return err
	}
	session := &session{client, inbox, output}
	if to.Type == "email" {
		receipt, lookupErr := client.ResolveEmail(ctx, callID(), to.ID)
		lookup, lookupErr := session.await(ctx, receipt, lookupErr)
		if lookupErr != nil {
			return lookupErr
		}
		var found bool
		to, found = lookup.ResolvedRecipient()
		if !found {
			return errUnresolvedRecipient
		}
	}
	if welcome != nil {
		receipt, err := client.Send(ctx, callID(), to, uuid.NewString(), welcome.card)
		if _, err = session.await(ctx, receipt, err); err != nil {
			return err
		}
		if welcome.table == nil {
			return nil
		}
		receipt, err = client.UploadFile(ctx, callID(), lark.Upload{Name: "repository-snapshot.csv", Type: "stream", Data: welcome.table})
		file, err := session.await(ctx, receipt, err)
		if err != nil {
			return err
		}
		content, err := lark.NewContent("file", []byte(fmt.Sprintf(`{"file_key":%q}`, file.AssetKey())))
		if err != nil {
			return err
		}
		receipt, err = client.Send(ctx, callID(), to, uuid.NewString(), content)
		_, err = session.await(ctx, receipt, err)
		return err
	}
	var messages []string
	defer func() {
		if !exercise {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for index := len(messages) - 1; index >= 0; index-- {
			receipt, err := client.Recall(cleanup, callID(), messages[index])
			_, err = session.await(cleanup, receipt, err)
			resultErr = errors.Join(resultErr, err)
		}
	}()
	_, post, image, table, err := report("")
	if err != nil {
		return err
	}
	receipt, err := client.UploadImage(ctx, callID(), "synthetic-chart.png", image)
	uploaded, err := session.await(ctx, receipt, err)
	if err != nil {
		return err
	}
	card, _, _, _, err := report(uploaded.AssetKey())
	if err != nil {
		return err
	}
	receipt, err = client.Send(ctx, callID(), to, uuid.NewString(), card)
	sent, err := session.await(ctx, receipt, err)
	if sent.MessageID() != "" {
		messages = append(messages, sent.MessageID())
	}
	if err != nil {
		return err
	}
	receipt, err = client.UploadFile(ctx, callID(), lark.Upload{Name: "synthetic-data.csv", Type: "stream", Data: table})
	file, err := session.await(ctx, receipt, err)
	if err != nil {
		return err
	}
	content, err := lark.NewContent("file", []byte(fmt.Sprintf(`{"file_key":%q}`, file.AssetKey())))
	if err != nil {
		return err
	}
	receipt, err = client.Send(ctx, callID(), to, uuid.NewString(), content)
	attached, err := session.await(ctx, receipt, err)
	if attached.MessageID() != "" {
		messages = append(messages, attached.MessageID())
	}
	if err != nil {
		return err
	}
	if !exercise {
		return nil
	}
	receipt, err = client.GetMessage(ctx, callID(), sent.MessageID())
	inspected, err := session.await(ctx, receipt, err)
	if err != nil {
		return err
	}
	if len(inspected.Messages()) != 1 || inspected.Messages()[0].ID() != sent.MessageID() {
		return errors.New("independent message identity mismatch")
	}
	receipt, err = client.PatchMessage(ctx, callID(), sent.MessageID(), card)
	if _, err = session.await(ctx, receipt, err); err != nil {
		return err
	}
	receipt, err = client.Reply(ctx, callID(), sent.MessageID(), uuid.NewString(), false, post)
	replied, err := session.await(ctx, receipt, err)
	if replied.MessageID() != "" {
		messages = append(messages, replied.MessageID())
	}
	if err != nil {
		return err
	}
	text, err := lark.Text("Fathomry integration exercise: this transient message will be recalled.")
	if err != nil {
		return err
	}
	receipt, err = client.Send(ctx, callID(), to, uuid.NewString(), text)
	transient, err := session.await(ctx, receipt, err)
	if transient.MessageID() != "" {
		messages = append(messages, transient.MessageID())
	}
	if err != nil {
		return err
	}
	receipt, err = client.UpdateMessage(ctx, callID(), transient.MessageID(), text)
	if _, err = session.await(ctx, receipt, err); err != nil {
		return err
	}
	receipt, err = client.DownloadResource(ctx, callID(), attached.MessageID(), file.AssetKey(), "file")
	downloaded, err := session.await(ctx, receipt, err)
	if err != nil {
		return err
	}
	if string(downloaded.Bytes()) != string(table) {
		return errors.New("downloaded attachment differs")
	}
	receipt, err = client.CreateCard(ctx, callID(), card)
	entity, err := session.await(ctx, receipt, err)
	if err != nil {
		return err
	}
	reference, err := lark.CardInstance(entity.CardID())
	if err != nil {
		return err
	}
	receipt, err = client.Send(ctx, callID(), to, uuid.NewString(), reference)
	instance, err := session.await(ctx, receipt, err)
	if instance.MessageID() != "" {
		messages = append(messages, instance.MessageID())
	}
	if err != nil {
		return err
	}
	revision := lark.Revision{Sequence: 1, UUID: uuid.NewString()}
	update, err := lark.NewJSON([]byte(`{"content":"**Verified:** this text was updated through CardKit. Synthetic data only."}`))
	if err != nil {
		return err
	}
	receipt, err = client.PatchElement(ctx, callID(), entity.CardID(), "intro", revision, update)
	if _, err = session.await(ctx, receipt, err); err != nil {
		return err
	}
	revision.Sequence++
	revision.UUID = uuid.NewString()
	receipt, err = client.UpdateCard(ctx, callID(), entity.CardID(), revision, card)
	if _, err = session.await(ctx, receipt, err); err != nil {
		return err
	}
	return nil
}
