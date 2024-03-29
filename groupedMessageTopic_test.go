package redimq

import (
	"fmt"
	"testing"
	// "github.com/go-redis/redis/v8"
)

func TestGMTPublishMessage(t *testing.T) {
	m := &Message{Data: map[string]interface{}{"foo": "test", "bar": "test"}}
	// args := &redis.XAddArgs{
	// 	Stream:"redimq:umts:" + topic.Name,
	// 	Values: m.Data,
	// 	NoMkStream: false,
	// }
	// mock.ExpectXAdd(args).SetVal("12345")
	err := gmt.PublishMessage("groupkey", m)
	if err != nil {
		t.Fatal("PublishMessage failed", err)
	}
	if m.Id == "" {
		t.Error("PublishMessage did not generate ID")
	}
	println(m.Id)
}

func TestGMTConsumeMessages(t *testing.T) {
	group := "test-group"
	consumer := "test-consumer"
	// args := &redis.XReadGroupArgs{
	// 	Group: group,
	// 	Consumer: consumer,
	// 	Count: int64(count),
	// 	Streams: []string { topic.StreamKey },
	// }
	// msg := &redis.XMessage{ID: "12345", Values: map[string]interface{} {"foo": "test", "bar": "test"}}
	// resStream := &redis.XStream{
	// 	Stream: topic.StreamKey,
	// 	Messages: []redis.XMessage { *msg },
	// }
	// mock.ExpectXReadGroup(args).SetVal([]redis.XStream { *resStream })
	msgs, err := gmt.ConsumeMessages(group, consumer)
	if err != nil {
		t.Error("ConsumeMessage failed", err)
	} else if msgs == nil {
		t.Error("ConsumeMessage did not return message")
	} else if msgs[0].Id == "" {
		t.Error("ConsumeMessage message does not match")
	}
	fmt.Println("messages consumed - ", *msgs[0])
}

func TestGMTCleanupMessageGroupsAndConsumers(t *testing.T) {
	group := "test-group"
	gmt.CleanupMessageGroupsAndConsumers(group)
}
