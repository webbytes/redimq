package redimq

import (
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestPublishMessageToGroupedMessageTopic(t *testing.T) {

} 

func TestConsumeMessagesFromGroupedMessageTopic(t *testing.T) {
	consumerGroupName := "test"
	consumerName := "test"
	count := int64(1)
	args := &redis.XReadGroupArgs{
		Group: consumerGroupName,
		Consumer: consumerName,
		Streams: []string { gmt.MessageGroupStreamKey },
		Count: 1,
	}
	msg := &redis.XMessage{ID: "12345", Values: map[string]interface{} {"key": "test"}}
	resStream := &redis.XStream{
		Stream: topic.StreamKey,
		Messages: []redis.XMessage { *msg },
	}
	mock.ExpectXReadGroup(args).SetVal([]redis.XStream { *resStream })
	mg, err := gmt.GetAndLockMessageGroup(consumerGroupName, consumerName)
	if err != nil {
		t.Error("GetAndLockMessageGroup failed", err)
	}
	args = &redis.XReadGroupArgs{
		Group: consumerGroupName,
		Consumer: consumerName,
		Count: int64(count),
		Streams: []string { mg.StreamKey },
	}
	msg = &redis.XMessage{ID: "12345", Values: map[string]interface{} {"foo": "test", "bar": "test"}}
	resStream = &redis.XStream{
		Stream: gmt.MessageGroupStreamKey,
		Messages: []redis.XMessage { *msg },
	}
	mock.ExpectXClaim(&redis.XClaimArgs{
		Stream: gmt.MessageGroupStreamKey,
		Group: consumerGroupName,
		Consumer: consumerName,
		Messages: []string { "12345" },
	})
	mock.ExpectXReadGroup(args).SetVal([]redis.XStream{ *resStream })
	msgs, err := mg.ConsumeMessage()
	if err != nil {
		t.Error("GetAndLockMessageGroup failed", err)
	}
	if msgs == nil {
		t.Error("ConsumeMessages returned nil")
	}
}