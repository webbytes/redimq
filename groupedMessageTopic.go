package redimq

type GroupedMessageTopic struct {
	Name string
	MQClient
}

func (t *GroupedMessageTopic) PublishMessage(m Message) error {
	return nil
}