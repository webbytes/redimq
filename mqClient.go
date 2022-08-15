package redimq

import (
	"context"
	"time"
	"github.com/go-redis/redis/v8"
)

// MQClient is the struct used for interacting with the queues that are created and 
// managed by RediMQ. It is created using the NewMQClient function of redimq.
//
//  func Main() {
//		rdb := redis.NewClient(&redis.Options{
//			Addr: ":6379",
//		})
//  	client, err := redimq.NewMQClient(content.TODO(), rdb)
// 	}
//
type MQClient struct {
	c context.Context
	rc *redis.Client
}

type TopicType string
const (
	UngroupedMessages TopicType = "umts"
	GroupedMessages  = "gmts"
)

func (c *MQClient) NewTopic(name string) (*Topic, error) {
	idle, _ := time.ParseDuration(DefaultMaxIdleTimeForMessage)
	topic := &Topic { StreamKey: "redimq:umts:" + name, Name: name, MQClient: *c, MaxIdleTimeForMessages: idle, NeedsAcknowledgements: true }
	_,err := c.rc.SAdd(c.c, "redimq:umts", topic.Name).Result()
	return topic, err
}

func (c *MQClient) NewGroupedMessageTopic(name string) (*GroupedMessageTopic, error) {
	topic := &GroupedMessageTopic { MessageGroupStreamKey: "redimq:gmts:" + name + ":message-groups", Name: name, MQClient: *c }
	_,err := c.rc.SAdd(c.c, "redimq:gmts", topic.Name).Result()
	return topic, err
}

func (c *MQClient) getTopics(topicType TopicType) ([]string, error) {
	key := "redimq:" + string(topicType)
	ts,err := c.rc.SMembers(c.c, key).Result()
	return ts, err
} 

func (c *MQClient) findTopics(topicType TopicType, pattern *string, count int64, cursor uint64) ([]string, uint64, error) {
	key := "redimq:" + string(topicType)
	defaultPattern := "*"
	if pattern == nil {
		pattern = &defaultPattern
	}
	ts,cur,err := c.rc.SScan(c.c, key, cursor, *pattern, count).Result()
	return ts, cur, err
}

func (c *MQClient) GetAllUngroupedMessageTopics() ([]*Topic, error) {
	ts, err := c.getTopics(UngroupedMessages)
	if err != nil {
		return nil, err
	}
	topics := make([]*Topic, len(ts))
	for i,t := range ts {
		topics[i] = &Topic{Name: t, MQClient: *c}
	}
	return topics, err
}

func (c *MQClient) GetAllGroupedMessageTopics() ([]*GroupedMessageTopic, error) {
	ts, err := c.getTopics(GroupedMessages)
	if err != nil {
		return nil, err
	}
	topics := make([]*GroupedMessageTopic, len(ts))
	for i,t := range ts {
		topics[i] = &GroupedMessageTopic{Name: t, MQClient: *c}
	}
	return topics, err
}

func (c *MQClient) FindUngroupedMessageTopics(pattern *string, count int64, cursor uint64) ([]*Topic, uint64, error) {
	ts, cur, err := c.findTopics(UngroupedMessages, pattern, count, cursor)
	if err != nil {
		return nil, 0, err
	}
	topics := make([]*Topic, len(ts))
	for i,t := range ts {
		topics[i] = &Topic{Name: t, MQClient: *c}
	}
	return topics, cur, err
}

func (c *MQClient) FindGroupedMessageTopics(pattern *string, count int64, cursor uint64) ([]*GroupedMessageTopic, uint64, error) {
	ts, cur, err := c.findTopics(GroupedMessages, pattern, count, cursor)
	if err != nil {
		return nil, 0, err
	}
	topics := make([]*GroupedMessageTopic, len(ts))
	for i,t := range ts {
		topics[i] = &GroupedMessageTopic{Name: t, MQClient: *c}
	}
	return topics, cur, err
}
