package redimq

const FUNC_PUBLISH_MESSAGE_GMT = "publishToGMT"

// #!lua name=rediMQLib
//  local function addMessageGroup(keys, args)
//  -- check if messageGroup exists in list
// 	if redis.call("LPOS", keys[1], args[1]) == nil then
// 		-- check if count of messageGroups is more than maxMessageGroups var
// 		if redis.call("LLEN", keys[1]) == maxMessageGroups then
// 			redis.call("LPOP", keys[1])
// 		end
// 		-- add to list and stream
// 		redis.call("LPUSH", keys[1], args[1]) 
// 		redis.call("XADD", "keys[2], "MAXLEN", "~", maxMessageGroups, "*", "messageGroupkey",args[1])
// 	end
// end
// redis.register_function('addMessageGroup', addMessageGroup)
//
//

// LUA Scripts that are going to be used for some of the operations that need to be atomic. These would 
// never need to be overridden by can if needed. Overriding these could change the behaviour of the library
// and could use the system in a non recoverable state. Please change this only if you are absolutely sure
// of what you are doing 
var (
	// FCALL publishToGMT 2 redimq:gmts:testQueue:messageGroups redimq:gmts:testQueue:messages:TEST_KEY 
	//	MAXLEN ~ 1000 key TEST_KEY
	//	MAXLEN ~ 1000 foo 1 bar 2
	LUA_publishToGMT = `
	local function publishToGMT(keys, args)
		if redis.call("EXISTS", keys[2]) == 0 then
			redis.call("XADD", keys[1], unpack(args, 1, 5))
		end
		redis.call("XADD", keys[2], unpack(args, 6))
	end
	`
	// FCALL consumeFromUMT 1 redimq:umts:testQueue consumerGroupName consumerName 1112121211221
	LUA_consumeFromUMT = `
	local function consumeFromUMT(keys, args)
		local msgs = redis.call("XAUTOCLAIM", keys[1], args[1], args[2], args[4], "0-0", 1)
		if msgs == nil then
			msgs = redis.call("XREADGROUP", "GROUP", keys[2], args[1], args[2], "COUNT", args[3], "STREAMS", keys[2])
		end
		return msgs
	end
	`
	// FCALL lockMessageGroups 1 redimq:gtms:testQueue:messageGroups consumerGroupName consumerName 10 300000
	LUA_lockMessageGroups = `
	local function lockMessageGroups(keys, args)
		mgs = redis.call("XREADGROUP", "GROUP", args[1], args[2], "COUNT", args[3], "STREAMS", keys[1])
		count = args[3]
		if msgs != nil then
			count = table.getn(mgs[0].Messages)
		end
		if count > 0
			local msgs = redis.call("XAUTOCLAIM", keys[1], args[1], args[2], args[4], "0-0", "COUNT", count)
		end
		return {unpack(mgs),unpack(msgs)}
	end
	`
	// FCALL getMessageGroupsPerConsumer 1 redimq:gtms:testQueue:messageGroups consumerGroupName consumerName
	LUA_getMessageGroupsPerConsumer = `
	local function getMessageGroups(keys, args)
		consumerCount = table.getn(redis.call("XINFO", "CONSUMERS", keys[1], args[1]))
		messageGroupCount = table.getn(redis.call("")
		mgPerConsumer = messageGroupCount / consumerCount
		return mgPerConsumer
	end
	`
	// FCALL consumeFromGMT 3 redimq:gmts:testQueue:messageGroups redimq:gmts:testQueue:messages:TEST_KEY 
	// 	redimq:gmts:testQueue:messageGroups:consumerGroupName:consumerName
	//	consumerGroupName consumerName 1112121211221 12345 TEST_KEY
	LUA_consumeFromGMT = `
	local function consumeFromGMT(keys, args)
		if redis.call("XPENDING", keys[1], args[1], args[4], args[4], args[2])) == nil then
			return redis.error_reply("The Message " + args[4] + " is not locked by consumer " + args[2] + " for consumer group " + args[1], 2)
		else
			local msgs = consumeFromUMT(unpack(keys,2), unpack(args,1,3))
			if msgs == nil then
				redis.call("XCLAIM", keys[1], args[1], args[2], args[4], 0, args[5], "IDLE", args[3])
				redis.call("PEXPIRE", keys[2], retention)
			else 
				redis.call("XCLAIM", keys[1], args[1], args[2], 0, args[4], "RETRYCOUNT", 0)
			end
		end
		return msgs
	end
	`
)
const FUNC_NEW_GMT = "newGMT"