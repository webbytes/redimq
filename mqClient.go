package redimq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// MQClient is the struct used for interacting with the queues that are created and
// managed by RediMQ. It is created using the NewMQClient function of redimq.
//
//	 func Main() {
//			rdb := redis.NewClient(&redis.Options{
//				Addr: ":6379",
//			})
//	 	client, err := redimq.NewMQClient(content.TODO(), rdb)
//		}
type MQClient struct {
	c  context.Context
	rc *redis.Client
}

type TopicOptions struct {
	MaxRetentionDuration   *string
	MaxLength              *int64
	MaxIdleTimeForMessages *string
}

type TopicType string

const (
	UngroupedMessages TopicType = "umts"
	GroupedMessages   TopicType = "gmts"
)

func (c *MQClient) NewTopic(name string, options *TopicOptions) (*Topic, error) {
	if options == nil {
		options = &TopicOptions{
			MaxIdleTimeForMessages: &DefaultMaxIdleTimeForMessage,
		}
	} else if options.MaxIdleTimeForMessages == nil {
		options.MaxIdleTimeForMessages = &DefaultMaxIdleTimeForMessage
	}
	idle, err := time.ParseDuration(*options.MaxIdleTimeForMessages)
	topic := &Topic{
		StreamKey:              "redimq:umts:" + name,
		Name:                   name,
		MQClient:               *c,
		MaxIdleTimeForMessages: idle,
		NeedsAcknowledgements:  true,
	}
	return topic, err
}

func (c *MQClient) NewGroupedMessageTopic(name string, options *TopicOptions) (*GroupedMessageTopic, error) {
	if options == nil {
		options = &TopicOptions{
			MaxIdleTimeForMessages: &DefaultMaxIdleTimeForMessage,
		}
	} else if options.MaxIdleTimeForMessages == nil {
		options.MaxIdleTimeForMessages = &DefaultMaxIdleTimeForMessage
	}
	idle, err := time.ParseDuration(*options.MaxIdleTimeForMessages)
	topic := &GroupedMessageTopic{
		StreamPrefix:           "redimq:gmts:" + name,
		MessageGroupStreamKey:  "redimq:gmts:" + name + ":message-groups",
		MessageCountKey:        "redimq:gmts:" + name + ":message-count",
		Name:                   name,
		MQClient:               *c,
		MaxIdleTimeForMessages: idle,
	}
	return topic, err
}

func (c *MQClient) NewConsumer(consumerGroupName string, consumerName string, handler func(*Message)) *Consumer {
	return &Consumer{
		ConsumerGroupName: consumerGroupName,
		ConsumerName:      consumerName,
		Handler:           handler,
		Errors:            make(chan error),
		inProgressTopic:   map[string]bool{},
	}
}
func (c *MQClient) createGroupAndConsumer(stream string, consumerGroupName string, consumerName string) error {
	// consumerKey := "redimq:gmts:" + topicName + ":cg:" + consumerGroupName + ":consumers:" + consumerName
	// duration, _ := time.ParseDuration("5m")
	// _,err := c.rc.SetNX(c.c, consumerKey, 1, 0).Result()
	// _,err = c.rc.Expire(c.c, consumerKey, duration).Result()
	_, err := c.rc.XGroupCreateMkStream(c.c, stream, consumerGroupName, "0").Result()
	_, err = c.rc.XGroupCreateConsumer(c.c, stream, consumerGroupName, consumerName).Result()
	return err
}

func (c *MQClient) getTopics(topicType TopicType) ([]string, error) {
	key := "redimq:" + string(topicType)
	ts, err := c.rc.SMembers(c.c, key).Result()
	return ts, err
}

func (c *MQClient) findTopics(topicType TopicType, pattern *string, count int64, cursor uint64) ([]string, uint64, error) {
	key := "redimq:" + string(topicType)
	defaultPattern := "*"
	if pattern == nil {
		pattern = &defaultPattern
	}
	ts, cur, err := c.rc.SScan(c.c, key, cursor, *pattern, count).Result()
	return ts, cur, err
}

func (c *MQClient) GetAllUngroupedMessageTopics() ([]*Topic, error) {
	ts, err := c.getTopics(UngroupedMessages)
	if err != nil {
		return nil, err
	}
	topics := make([]*Topic, len(ts))
	for i, t := range ts {
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
	for i, t := range ts {
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
	for i, t := range ts {
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
	for i, t := range ts {
		topics[i] = &GroupedMessageTopic{Name: t, MQClient: *c}
	}
	return topics, cur, err
}
