package redimq

import (
)

type MessageGroup struct {
	Id string
	Key string
	ConsumerGroupName string
	ConsumerName string
	Topic
}

func (mg *MessageGroup) ConsumeMessage() (*Message, error) {
	msgs, err := mg.ConsumeMessages(mg.ConsumerGroupName, mg.ConsumerName, 1)
	return msgs[0], err
}