package redimq

import (
	"github.com/go-redis/redis/v8"
)

type ScriptKeys string

// LUA Scripts that are going to be used for some of the operations that need to be atomic. These would
// never need to be overridden by can if needed. Overriding these could change the behaviour of the library
// and could leave the system in a non recoverable state. Please change this only if you are absolutely sure
// of what you are doing
var (
	LUA_publishToGMT ScriptKeys = `
		if redis.call("SADD", KEYS[1], ARGV[1]) == 1 then
			redis.call("XADD", KEYS[2], "*", "key", ARGV[1])
		end
		return "OK"
	`
	LUA_reclaimMessageGroups ScriptKeys = `
		local pending = redis.call("XPENDING", KEYS[1], ARGV[1], "-", "+", ARGV[3], ARGV[2])
		if pending == nil or table.getn(pending) == 0 then 
			return {}
		else
			local ids = {}
			for i=1,table.getn(pending) do
				ids[i] = pending[i][1]
			end
			return redis.call("XCLAIM", KEYS[1], ARGV[1], ARGV[2], 0, unpack(ids, 1))
		end
	`
	LUA_deleteMessageGroupIfEmpty ScriptKeys = `
		local msgCount = redis.call("XLEN", KEYS[2])
		if msgCount == 0 then
			redis.call("XDEL", KEYS[1], ARGV[1])
			redis.call("DEL", KEYS[2])
			return 1
		end
		return 0
	`
)

type Scripts struct {
	PublishToGMT              *redis.Script
	ReclaimMessageGroup       *redis.Script
	DeleteMessageGroupIfEmpty *redis.Script
}

var RediMQScripts *Scripts

func initializeScripts() {
	RediMQScripts = &Scripts{
		PublishToGMT:              redis.NewScript(string(LUA_publishToGMT)),
		ReclaimMessageGroup:       redis.NewScript(string(LUA_reclaimMessageGroups)),
		DeleteMessageGroupIfEmpty: redis.NewScript(string(LUA_deleteMessageGroupIfEmpty)),
	}
}
