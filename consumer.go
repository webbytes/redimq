package redimq

import (
	"time"
)

// Consumer can be used for consuming messages from a queue.
type Consumer struct {
	ConsumerGroupName string
	ConsumerName      string
	MaxIdleDuration   time.Duration
	Handler           func(m *Message)
	Messages          chan *Message
	Errors            chan error
}

func (c *Consumer) StartConsumingTopic(t *Topic, count int64) error {
	_, err := t.MQClient.rc.XGroupCreate(t.MQClient.c, t.StreamKey, c.ConsumerGroupName, "0-0").Result()
	if err != nil {
		println("group creation error - ", err.Error())
	}
	for {
		msgs, err := t.ConsumeMessages(c.ConsumerGroupName, c.ConsumerName, count)
		if err != nil {
			c.Errors <- err
		}
		if len(msgs) > 0 {
			for _, m := range msgs {
				if c.Handler == nil {
					c.Messages <- m
				} else {
					c.Handler(m)
				}
			}
		}
	}
}

// StartConsumingGroupedMessageTopic function will start continuously reading from the queue passed in as
// argument and call the handler function for each of the messages. If the Handler function is not set then
// the function will send the messages into the [Consumer.Message] channel. Any errors encountered would be
// sent into the [Consumer.Errors] channel
func (c *Consumer) StartConsumingGroupedMessageTopic(t *GroupedMessageTopic) {
	t.MQClient.createGroupAndConsumer(t.MessageGroupStreamKey, c.ConsumerGroupName, c.ConsumerName)
	for {
		msgs, err := t.ConsumeMessages(c.ConsumerGroupName, c.ConsumerName)
		if err != nil {
			c.Errors <- err
		}
		if len(msgs) > 0 {
			for _, m := range msgs {
				if c.Handler == nil {
					c.Messages <- m
				} else {
					c.Handler(m)
				}
			}
		}
	}
}
