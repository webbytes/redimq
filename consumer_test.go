package redimq

import (
	"fmt"
	"testing"
)

func TestConsumerStartConsumingTopic(t *testing.T) {
	consumer := client.NewConsumer("test-group", "test-consumer", func(m *Message) {
		fmt.Println(m)
		if m.Id == "" {
			t.Error("Message consumed is not valid")
		}
		m.Acknowledge()
	})
	consumer.StartConsumingTopic(topic, 1)
	select {
	case err := <-consumer.Errors:
		t.Error("Error consuming Topic", err)
	default:
		fmt.Println("No errors")
	}

}

func TestConsumerStartConsumingGroupedMessageTopic(t *testing.T) {
	consumer := client.NewConsumer("test-group", "test-consumer", func(m *Message) {
		fmt.Println(m)
		if m.Id == "" {
			t.Error("Message consumed is not valid")
		}
		m.Acknowledge()
	})
	consumer.StartConsumingGroupedMessageTopic(gmt)
	select {
	case err := <-consumer.Errors:
		t.Error("Error consuming GroupedMessageTopic", err)
	default:
		fmt.Println("No errors")
	}
}
