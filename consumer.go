package redimq

import (
	"fmt"
	"time"
)

type Consumer struct {
	ConsumerGroupName string
	ConsumerName string
	MaxIdleDuration time.Duration
	Handler func (m *Message) error 
}

func (c *Consumer) StartConsumingTopic(t *Topic) error {
	for {
		m, err := t.ConsumeMessages(c.ConsumerGroupName, c.ConsumerName, 1)
		if err != nil {
			fmt.Println("Error consuming message for ", c, err)
		}
		if m != nil && len(m) == 1 {
			err = c.Handler(m[0])
		}
		if err == nil {
			err = m[0].Acknowledge()
		}
	}
}

func (c *Consumer) StartConsumingGroupedMessageTopic(t *GroupedMessageTopic) error {
	for {
		msgs, err := t.ConsumeMessages(c.ConsumerGroupName, c.ConsumerName)
		if err != nil {
			fmt.Println("Error consuming message for ", c, err)
		}
		if msgs != nil && len(msgs) > 0 {
			for _,m := range msgs {
				err = c.Handler(m)
				if err == nil {
					err = m.Acknowledge()
				}
			}
		}
	}
	return nil
}