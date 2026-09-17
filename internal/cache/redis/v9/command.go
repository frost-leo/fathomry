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

package redis

import (
	"context"
	"errors"
	"math/big"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/redis/go-redis/v9"
)

// Command is an immutable, binary-safe argument vector. Strings may contain
// arbitrary bytes; empty is not missing. The constructor does not grant authority.
// WithKeyPosition is for module commands whose routing metadata is unavailable.
type Command struct {
	private
	args        []string
	keyPosition int8
}

func NewCommand(args ...string) Command {
	if len(args) > 65536 {
		return Command{}
	}
	return Command{args: append([]string(nil), args...)}
}
func (command Command) WithKeyPosition(position int8) Command {
	command.keyPosition = position
	return command
}
func validGrant(grant string) bool {
	if len(grant) == 0 || len(grant) > 80 {
		return false
	}
	parts := strings.Split(grant, "|")
	if len(parts) > 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, letter := range part {
			if !(letter >= 'A' && letter <= 'Z' || letter >= '0' && letter <= '9' || letter == '.' || letter == '_' || letter == '-') {
				return false
			}
		}
	}
	return true
}
func forbidden(name string) bool {
	switch name {
	case "AUTH", "HELLO", "SELECT", "CLIENT", "RESET", "QUIT", "MONITOR", "SYNC", "PSYNC", "REPLCONF",
		"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH", "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE",
		"UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE", "READONLY", "READWRITE", "ASKING":
		return true
	}
	return false
}
func ordinary(name string) bool {
	// This is an authority classification, not a server-version support claim.
	const names = " PING ECHO GET GETSET GETDEL GETEX GETRANGE SUBSTR LCS SET SETEX PSETEX SETNX SETRANGE MGET MSET MSETEX MSETNX APPEND STRLEN INCR INCRBY INCRBYFLOAT DECR DECRBY " +
		"DEL UNLINK EXISTS TYPE DUMP RESTORE RENAME RENAMENX COPY EXPIRE PEXPIRE EXPIREAT PEXPIREAT EXPIRETIME PEXPIRETIME TTL PTTL PERSIST TOUCH " +
		"HGET HSET HSETNX HMGET HMSET HGETALL HDEL HEXISTS HLEN HKEYS HVALS HSTRLEN HINCRBY HINCRBYFLOAT HRANDFIELD HSCAN HEXPIRE HPEXPIRE HEXPIREAT HPEXPIREAT HEXPIRETIME HPEXPIRETIME HTTL HPTTL HPERSIST HGETEX HSETEX HGETDEL " +
		"LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LLEN LRANGE LINDEX LINSERT LSET LTRIM LREM LPOS LMOVE LMPOP RPOPLPUSH BLPOP BRPOP BLMOVE BLMPOP BRPOPLPUSH LMOVEM BLMOVEM " +
		"SADD SREM SMEMBERS SCARD SISMEMBER SMISMEMBER SPOP SRANDMEMBER SMOVE SINTER SUNION SDIFF SINTERSTORE SUNIONSTORE SDIFFSTORE SINTERCARD SUNIONCARD SDIFFCARD SSCAN " +
		"ZADD ZREM ZCARD ZCOUNT ZINCRBY ZSCORE ZMSCORE ZRANK ZREVRANK ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX ZLEXCOUNT ZPOPMIN ZPOPMAX BZPOPMIN BZPOPMAX ZMPOP BZMPOP ZRANDMEMBER ZDIFF ZDIFFSTORE ZINTER ZINTERCARD ZINTERSTORE ZUNION ZUNIONSTORE ZRANGESTORE ZSCAN " +
		"SETBIT GETBIT BITCOUNT BITPOS BITOP BITFIELD BITFIELD_RO GEOADD GEODIST GEOHASH GEOPOS GEORADIUS GEORADIUS_RO GEORADIUSBYMEMBER GEORADIUSBYMEMBER_RO GEOSEARCH GEOSEARCHSTORE PFADD PFCOUNT PFMERGE " +
		"SCAN SORT SORT_RO XADD XDEL XLEN XRANGE XREVRANGE XREAD XREADGROUP XGROUP XINFO XACK XPENDING XCLAIM XAUTOCLAIM XTRIM XSETID XACKDEL XDELEX " +
		"PUBLISH SPUBLISH PUBSUB EVAL EVAL_RO EVALSHA EVALSHA_RO FCALL FCALL_RO WAIT WAITAOF "
	return strings.Contains(names, " "+name+" ")
}
func (value settings) authorized(command Command, dedicated bool) bool {
	if len(command.args) == 0 || len(command.args[0]) == 0 || len(command.args[0]) > 80 {
		return false
	}
	name := strings.ToUpper(command.args[0])
	subcommand := ""
	if len(command.args) > 1 && len(command.args[1]) <= 80 {
		subcommand = name + "|" + strings.ToUpper(command.args[1])
	}
	if forbidden(name) || name == "HIMPORT" && !dedicated {
		return false
	}
	if name == "SCAN" && !dedicated && (value.mode() == "cluster" || value.mode() == "ring") {
		return false
	}
	for _, grants := range [][]string{value.Commands, value.AdminCommands} {
		for _, grant := range grants {
			if grant == name || grant == subcommand {
				return true
			}
		}
	}
	return false
}
func (value settings) check(commands []Command, dedicated bool) error {
	if len(commands) == 0 || len(commands) > value.MaxCommands {
		return failure(ErrLimit, "commands")
	}
	total := 0
	for _, command := range commands {
		if len(command.args) == 0 || len(command.args) > value.MaxArgs || command.keyPosition < 0 ||
			int(command.keyPosition) >= len(command.args) {
			return failure(ErrInput, "arguments")
		}
		if !value.authorized(command, dedicated) {
			return failure(ErrAuthority, "command")
		}
		for _, arg := range command.args {
			if len(arg) > value.MaxRequestBytes-total-32 {
				return failure(ErrLimit, "arguments")
			}
			total += len(arg) + 32
		}
		if _, err := command.routingPosition(); err != nil {
			return err
		}
	}
	return nil
}
func (command Command) native(ctx context.Context) *sdk.Cmd {
	args := command.nativeArguments()
	cmd := sdk.NewCmd(ctx, args...)
	cmd.SetVal(unobservedReply)
	if position, _ := command.routingPosition(); position != 0 {
		cmd.SetFirstKeyPos(position)
	}
	return cmd
}

// The pinned generic RESP decoder never emits int32. Numeric zero also passes
// native fan-out aggregators without replacing an original node error with a
// marker-conversion error. Requalify this distinction with SDK upgrades.
const unobservedReply int32 = 0

// ReplyState reports technical evidence, never business success or retry safety.
type ReplyState uint8

const (
	NotEntered ReplyState = iota
	Unknown
	Replied
	// CacheOrReply means the experimental cache can satisfy this read locally.
	CacheOrReply
	// TransactionAborted means EXEC returned the native watch-conflict sentinel.
	TransactionAborted
)

// Reply retains one command's position, native error and immutable data.
// A server error does not prove that a script or module made no partial mutation.
type Reply struct {
	private
	state ReplyState
	value Value
	err   error
}

func (reply Reply) State() ReplyState { return reply.state }
func (reply Reply) Value() Value      { return reply.value }
func (reply Reply) Err() error        { return reply.err }

// Result is immutable and concurrently readable. Commands returns independent
// slice storage; neither native Cmder nor mutable values are published.
type Result struct {
	private
	replies []Reply
}

func (result Result) Commands() []Reply { return append([]Reply(nil), result.replies...) }

func collect(ctx context.Context, commands []*sdk.Cmd, entered, cached bool, limits settings, executionErr error) (Result, error) {
	result := Result{replies: make([]Reply, len(commands))}
	var bounds error
	for index, cmd := range commands {
		reply := Reply{state: NotEntered}
		if entered {
			reply.state = Unknown
			raw, err := cmd.Result()
			_, untouched := raw.(int32)
			if untouched {
				raw = nil
				if err == nil {
					err = executionErr
				}
				if err == nil {
					err = failure(ErrProtocol, "unobserved-reply")
				}
				bounds = errors.Join(bounds, err)
			}
			var server sdk.Error
			switch {
			case errors.Is(err, sdk.TxFailedErr):
				reply.state = TransactionAborted
			case !untouched && (err == nil || errors.Is(err, sdk.Nil) || errors.As(err, &server)):
				reply.state = Replied
			}
			if cached && cacheEligible(cmd.Name()) && reply.state == Replied {
				reply.state = CacheOrReply
			}
			boundErr := checkReply(raw, limits.MaxReplyBytes, limits.MaxReplyElements)
			if boundErr == nil {
				reply.value = freeze(raw)
			}
			bounds = errors.Join(bounds, boundErr)
			reply.err = nativeFailure(ctx, errors.Join(err, boundErr))
		}
		result.replies[index] = reply
	}
	return result, bounds
}

func checkReply(raw any, maxBytes, maxElements int) error {
	bytes, elements := 0, 0
	var visit func(any, int) error
	visit = func(raw any, depth int) error {
		elements++
		if depth > 32 || elements > maxElements {
			return failure(ErrLimit, "reply")
		}
		size := 0
		switch raw := raw.(type) {
		case nil:
		case string:
			size = len(raw)
		case []byte:
			size = len(raw)
		case int64, float64, bool:
			size = 8
		case *big.Int:
			if raw == nil {
				return failure(ErrProtocol, "reply")
			}
			size = raw.BitLen()/3 + 2
		case error:
			size = len(raw.Error())
		case []any:
			if len(raw) > maxElements-elements {
				return failure(ErrLimit, "reply")
			}
			for _, item := range raw {
				if err := visit(item, depth+1); err != nil {
					return err
				}
			}
		case map[any]any:
			if len(raw) > (maxElements-elements)/2 {
				return failure(ErrLimit, "reply")
			}
			for key, item := range raw {
				if err := visit(key, depth+1); err != nil {
					return err
				}
				if err := visit(item, depth+1); err != nil {
					return err
				}
			}
		default:
			return failure(ErrProtocol, "reply")
		}
		if size > maxBytes-bytes {
			return failure(ErrLimit, "reply")
		}
		bytes += size
		return nil
	}
	return visit(raw, 0)
}

// The pinned SDK's CSC command set determines possible cache provenance, not
// whether this particular concurrent invocation actually hit the cache.
func cacheEligible(name string) bool {
	const names = " get mget getbit getrange strlen substr hget hgetall hmget hkeys hvals hlen hexists hstrlen " +
		"lindex llen lpos lrange scard sismember smembers smismember sdiff sinter sintercard sunion " +
		"zcard zcount zlexcount zmscore zrange zrangebylex zrangebyscore zrank zrevrange zrevrangebylex zrevrangebyscore zrevrank zscore zdiff zinter zunion " +
		"bitcount bitfield_ro bitpos exists type sort_ro lcs geodist geohash geopos geosearch georadiusbymember_ro georadius_ro " +
		"xlen xrange xrevrange json.get json.mget json.arrindex json.arrlen json.objkeys json.objlen json.resp json.strlen json.type ts.get ts.info ts.range ts.revrange "
	return strings.Contains(names, " "+name+" ")
}

// ValueKind preserves null, empty, zero and aggregate distinctions.
type ValueKind uint8

const (
	Null ValueKind = iota
	Text
	Integer
	Double
	Boolean
	Array
	Map
	ErrorValue
	BigInteger
)

// Value owns a frozen native reply. Text uses a binary-safe immutable string;
// Bytes, Elements and Pairs return independent outer storage.
type Value struct {
	private
	kind     ValueKind
	text     string
	integer  int64
	double   float64
	boolean  bool
	elements []Value
	pairs    []Pair
	err      error
}
type Pair struct {
	private
	key, value Value
}

func (pair Pair) Key() Value                { return pair.key }
func (pair Pair) Value() Value              { return pair.value }
func (value Value) Kind() ValueKind         { return value.kind }
func (value Value) Text() (string, bool)    { return value.text, value.kind == Text }
func (value Value) Bytes() ([]byte, bool)   { return []byte(value.text), value.kind == Text }
func (value Value) Integer() (int64, bool)  { return value.integer, value.kind == Integer }
func (value Value) Double() (float64, bool) { return value.double, value.kind == Double }

// BigInteger returns the exact decimal representation without a mutable big.Int.
func (value Value) BigInteger() (string, bool) { return value.text, value.kind == BigInteger }
func (value Value) Boolean() (bool, bool)      { return value.boolean, value.kind == Boolean }
func (value Value) Elements() []Value          { return append([]Value(nil), value.elements...) }
func (value Value) Pairs() []Pair              { return append([]Pair(nil), value.pairs...) }
func (value Value) Err() error                 { return value.err }
func freeze(raw any) Value {
	switch raw := raw.(type) {
	case nil:
		return Value{}
	case string:
		return Value{kind: Text, text: raw}
	case []byte:
		return Value{kind: Text, text: string(raw)}
	case int64:
		return Value{kind: Integer, integer: raw}
	case *big.Int:
		if raw == nil {
			return Value{kind: ErrorValue, err: failure(ErrProtocol, "reply")}
		}
		return Value{kind: BigInteger, text: raw.String()}
	case float64:
		return Value{kind: Double, double: raw}
	case bool:
		return Value{kind: Boolean, boolean: raw}
	case []any:
		values := make([]Value, len(raw))
		for index, item := range raw {
			values[index] = freeze(item)
		}
		return Value{kind: Array, elements: values}
	case map[any]any:
		pairs := make([]Pair, 0, len(raw))
		for key, item := range raw {
			pairs = append(pairs, Pair{key: freeze(key), value: freeze(item)})
		}
		return Value{kind: Map, pairs: pairs}
	case error:
		return Value{kind: ErrorValue, err: failure(ErrCommand, "reply", raw)}
	default:
		return Value{kind: ErrorValue, err: failure(ErrProtocol, "reply")}
	}
}

// Execute sends one authorized command. Blocking commands occupy this same
// bounded synchronous call; use an appropriately bounded source timeout.
func (client *Client) Execute(ctx context.Context, id fault.Correlation, command Command) (*invocation.Receipt[Result], error) {
	return client.execute(ctx, id, []Command{command}, false, nil, nil, false)
}

type processor interface {
	Process(context.Context, sdk.Cmder) error
	Pipeline() sdk.Pipeliner
	TxPipeline() sdk.Pipeliner
}

func (client *Client) execute(ctx context.Context, id fault.Correlation, commands []Command, batch bool, parent *invocation.Scope, native processor, transaction bool) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil {
		return nil, failure(ErrInput, "call")
	}
	if err := client.owner.settings.check(commands, parent != nil); err != nil {
		return nil, err
	}
	name := "command"
	if batch {
		name = "pipeline"
	}
	if transaction {
		name = "transaction"
	}
	call, err := client.begin(ctx, id, name, invocation.Finite, parent)
	if err != nil {
		return nil, err
	}
	work, cancel, err := client.work(ctx)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	defer cancel()
	cmds := make([]*sdk.Cmd, len(commands))
	for index, command := range commands {
		cmds[index] = command.native(work)
	}
	if native == nil {
		native = client.owner.native
	}
	entered := false
	if work.Err() != nil {
		err = work.Err()
	} else {
		entered = true
		_, _ = call.Attempt()
		if batch {
			pipe := native.Pipeline()
			if transaction {
				pipe = native.TxPipeline()
			}
			for _, cmd := range cmds {
				_ = pipe.Process(work, cmd)
			}
			_, err = pipe.Exec(work)
		} else {
			err = native.Process(work, cmds[0])
		}
	}
	result, bounds := collect(work, cmds, entered, parent == nil && !batch && client.owner.settings.ExperimentalCache, client.owner.settings, err)
	call.Complete(invocation.Outcome[Result]{Present: entered, Value: result, Primary: nativeFailure(work, errors.Join(err, bounds))})
	return call.Receipt(), nil
}
