package redimq

import (
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestPublishMessageToTopic(t *testing.T) {
	m := &Message{Data: map[string]interface{} {"foo": "test", "bar": "test"}}
	args := &redis.XAddArgs{
		Stream:"redimq:umts:" + topic.Name, 
		Values: m.Data,
		NoMkStream: false,
	}
	mock.ExpectXAdd(args).SetVal("12345")
	err := topic.PublishMessage(m)
	if err != nil {
		t.Error("PublishMessage failed", err)
	}
	if m.Id != "12345" {
		t.Error("PublishMessage did not generate ID")
	}
} 

func TestConsumeMessagesFromTopic(t *testing.T) {
	group := "test-group"
	consumer := "test-consumer"
	count := 1
	args := &redis.XReadGroupArgs{
		Group: group,
		Consumer: consumer,
		Count: int64(count),
		Streams: []string { topic.StreamKey },
	}
	msg := &redis.XMessage{ID: "12345", Values: map[string]interface{} {"foo": "test", "bar": "test"}}
	resStream := &redis.XStream{
		Stream: topic.StreamKey,
		Messages: []redis.XMessage { *msg },
	}
	mock.ExpectXReadGroup(args).SetVal([]redis.XStream { *resStream })
	msgs,err := topic.ConsumeMessages(group, consumer, 1)
	if err != nil {
		t.Error("ConsumeMessage failed", err)
	}
	if msgs == nil {
		t.Error("ConsumeMessage did not return message")
	}
	if len(msgs) != count {
		t.Error("ConsumeMessage did not return correct number of messages")
	}
	if msgs[0].Id != "12345" {
		t.Error("ConsumeMessage message does not match")
	}
} 