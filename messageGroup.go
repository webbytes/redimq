package redimq

import (
	"github.com/go-redis/redis/v8"
)
type MessageGroup struct {
	Id string
	Key string
	ConsumerGroupName string
	ConsumerName string
	Topic
}

func (mg *MessageGroup) ConsumeMessage() (*Message, error) {
	res, err := mg.Topic.MQClient.rc.Do(mg.Topic.MQClient.c, "FCALL","consumeGMTS", 1, mg.Topic.StreamKey, mg.Topic.MaxIdleTimeForMessages).Result()
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	msgs := xMessageToMessage(res.([]redis.XMessage)[0], mg.Topic, mg.ConsumerGroupName, mg.ConsumerName)
	return msgs, err
}