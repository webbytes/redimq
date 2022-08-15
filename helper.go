package redimq

import (
	"time"
	"github.com/go-redis/redis/v8"
)


func readNewStreamMessages(client MQClient, consumerGroupName string, consumerName string, count int64, stream string) ([]redis.XMessage, error) {
	args := &redis.XReadGroupArgs{
		Group: consumerGroupName,
		Consumer: consumerName,
		Count: count,
		Streams: []string{stream},
	}
	res, err := client.rc.XReadGroup(client.c, args).Result()
	if err != nil {
		return nil, err
	}
	if res == nil || len(res) == 0 || res[0].Messages == nil {
		return []redis.XMessage{}, nil
	}
	return res[0].Messages, nil
}

func claimStuckStreamMessages(client MQClient, consumerGroupName string, consumerName string, count int64, stream string, idle time.Duration) ([]redis.XMessage, error) {
	args := &redis.XAutoClaimArgs{
		Stream: stream,
		Group: consumerGroupName,
		Consumer: consumerName,
		Count: count,
		Start: "0-0",
		MinIdle: idle,
	}
	res, _, err := client.rc.XAutoClaim(client.c, args).Result()
	if err != nil {
		return nil, err
	}
	if res == nil || len(res) == 0 {
		return []redis.XMessage{}, nil
	}
	return res, err
}

func xMessageToMessage(s redis.XMessage, t Topic, consumerGroupName string, consumerName string) *Message {
	return &Message {
		Id: s.ID,
		Data: s.Values,
		Topic: t,
		ConsumerGroupName: consumerGroupName,
		ConsumerName: consumerName,
	}
}

func xMessageArrayToMessageArray(xms []redis.XMessage, t Topic, consumerGroupName string, consumerName string) []*Message {
	msgs := make([]*Message, len(xms))
	for i,s := range xms {
		msgs[i] = xMessageToMessage(s, t, consumerGroupName, consumerName)
	}
	return msgs
}