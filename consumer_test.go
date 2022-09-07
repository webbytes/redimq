package redimq

import (
	"fmt"
	"testing"
)

func TestConsumerStartConsumingTopicWithHandler(t *testing.T) {
	consumer := client.NewConsumer("test-group", "test-consumer", func(m *Message) {
		fmt.Println(m)
		if m.Id == "" {
			t.Error("Message consumed is not valid")
		}
		m.Acknowledge()
	})
	go consumer.StartConsumingTopic(topic, 1)
	select {
	case err := <-consumer.Errors:
		t.Error("Error consuming Topic", err)
	default:
		fmt.Println("No errors")
	}

}

func TestConsumerStartConsumingGroupedMessageTopicWithHandler(t *testing.T) {
	consumer := client.NewConsumer("test-group", "test-consumer", func(m *Message) {
		fmt.Println(m)
		if m.Id == "" {
			t.Error("Message consumed is not valid")
		}
		m.Acknowledge()
	})
	go consumer.StartConsumingGroupedMessageTopic(gmt)
	select {
	case err := <-consumer.Errors:
		t.Error("Error consuming GroupedMessageTopic", err)
	default:
		fmt.Println("No errors")
	}
}

func TestConsumerStartConsumingTopicWithChannel(t *testing.T) {
	consumer := client.NewConsumer("test-group", "test-consumer", nil)
	go consumer.StartConsumingTopic(topic, 1)
	select {
	case msg := <-consumer.Messages:
		if msg.Id == "" {
			t.Error("Message consumed is not valid")
		}
		err := msg.Acknowledge()
		if err != nil {
			t.Error("Error acknowledging message to Topic", err)
		}
	case err := <-consumer.Errors:
		t.Error("Error consuming Topic", err)
	default:
		fmt.Println("No errors")
	}

}

func TestConsumerStartConsumingGroupedMessageTopicWithChannel(t *testing.T) {
	consumer := client.NewConsumer("test-group", "test-consumer", nil)
	go consumer.StartConsumingGroupedMessageTopic(gmt)
	select {
	case msg := <-consumer.Messages:
		if msg.Id == "" {
			t.Error("Message consumed is not valid")
		}
		err := msg.Acknowledge()
		if err != nil {
			t.Error("Error acknowledging message to GroupedMessageTopic", err)
		}
	case err := <-consumer.Errors:
		t.Error("Error consuming GroupedMessageTopic", err)
	default:
		fmt.Println("No errors")
	}
}
