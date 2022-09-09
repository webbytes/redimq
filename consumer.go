package redimq

import (
	"errors"
	"sync"
	"time"
)

// Consumer can be used for consuming messages from a queue. It can consume messages from both
// [Topic] and [GroupedMessageTopic]
//
// #Example for using [Consumer]
//
//	consumer := client.NewConsumer("test-group", "test-consumer", func(m *Message) {
//		// TODO: Process the message
//		m.Acknowledge()
//	})
//
// consumer.StartConsumingTopic(topic, 1)
// select {
// case err := <-consumer.Errors:
//
//	fmt.Println("Error consuming Topic", err)
//
// default:
//
//		fmt.Println("No errors")
//	}
type Consumer struct {
	ConsumerGroupName string
	ConsumerName      string
	MaxIdleDuration   time.Duration
	Handler           func(m *Message)
	BatchHandler      func(msgs []*Message)
	Errors            chan error
	inProgressTopic   map[string]bool
}

func (c *Consumer) consumeMessages(msgs []*Message) {
	var wg sync.WaitGroup
	for i := range msgs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Handler(msgs[i])
		}()
	}
	wg.Wait()
}

func (c *Consumer) consumeMessagesInBatches(msgs [][]*Message) {
	var wg sync.WaitGroup
	for i := range msgs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.BatchHandler(msgs[i])
		}()
	}
	wg.Wait()
}

func (c *Consumer) StartConsumingTopic(t *Topic, count int64) error {
	if c.Handler == nil {
		return errors.New("Consumer Handler is not set")
	}
	if c.inProgressTopic["umts:"+t.Name] {
		return errors.New("Topic is already being consumed")
	}
	_, err := t.MQClient.rc.XGroupCreate(t.MQClient.c, t.StreamKey, c.ConsumerGroupName, "0-0").Result()
	if err != nil {
		println("group creation error - ", err.Error())
	}
	go func() {
		c.inProgressTopic["umts:"+t.Name] = true
		for {
			if c.inProgressTopic[t.Name] {
				break
			}
			msgs, err := t.ConsumeMessages(c.ConsumerGroupName, c.ConsumerName, count)
			if err != nil {
				c.Errors <- err
			}
			if len(msgs) > 0 {
				c.consumeMessages(msgs)
			}
		}
	}()
	return nil
}

// StartConsumingGroupedMessageTopic function will start continuously reading from the queue passed in as
// argument and call the handler function for each of the messages. Any errors encountered would be
// sent into the [Consumer.Errors] channel. The Hander function is called in parallel for all messages
// retrieved as go routines. This would allow the consumer to process the messages faster.
func (c *Consumer) StartConsumingGroupedMessageTopic(t *GroupedMessageTopic) error {
	if c.Handler == nil {
		return errors.New("Consumer Handler is not set")
	}
	if c.inProgressTopic["gmts:"+t.Name] {
		return errors.New("GroupedMessageTopic is already being consumed")
	}
	t.MQClient.createGroupAndConsumer(t.MessageGroupStreamKey, c.ConsumerGroupName, c.ConsumerName)
	go func() {
		c.inProgressTopic["gmts:"+t.Name] = true
		for {
			if c.inProgressTopic["gmts"+t.Name] {
				break
			}
			msgs, err := t.ConsumeMessages(c.ConsumerGroupName, c.ConsumerName)
			if err != nil {
				c.Errors <- err
			}
			if len(msgs) > 0 {
				c.consumeMessages(msgs)
			}
		}
	}()
	return nil
}

// StartConsumingGroupedMessageTopicInBatches function works similar to the [StartConsumingGroupedMessageTopic]
// with the exception being that it can consume multiple messages from the same message group. This can cause the
// consumer to miss the order of the processing but is helpful if any summarization of the messages is needed at
// the message group level. The BatchHandler function is called for each of the message groups consumed. Any
// errors encountered would be sent into the [Consumer.Errors] channel
func (c *Consumer) StartConsumingGroupedMessageTopicInBatches(t *GroupedMessageTopic, batchSize int64) error {
	if c.BatchHandler == nil {
		return errors.New("Consumer BatchHandler is not set")
	}
	if c.inProgressTopic["gmts:"+t.Name] {
		return errors.New("GroupedMessageTopic is already being consumed")
	}
	t.MQClient.createGroupAndConsumer(t.MessageGroupStreamKey, c.ConsumerGroupName, c.ConsumerName)
	go func() {
		c.inProgressTopic["gmts:"+t.Name] = true
		for {
			if c.inProgressTopic["gmts"+t.Name] {
				break
			}
			msgs, err := t.ConsumeMessagesInBatches(c.ConsumerGroupName, c.ConsumerName, batchSize)
			if err != nil {
				c.Errors <- err
			}
			if len(msgs) > 0 {
				c.consumeMessagesInBatches(msgs)
			}
		}
	}()
	return nil
}

// StopConsumingTopic function is used to stop the consumption of messages. This can be restarted again by
// calling the [StartConsumingTopic] function
func (c *Consumer) StopConsumingTopic(t *Topic) {
	c.inProgressTopic["umts"+t.Name] = false
}

// StopConsumingGroupedMessageTopic function is used to stop the consumption of messages. This can be restarted
// again by calling the [StartConsumingGroupedMessageTopic] function
func (c *Consumer) StopConsumingGroupedMessageTopic(t *GroupedMessageTopic) {
	c.inProgressTopic["gmts"+t.Name] = false
}
