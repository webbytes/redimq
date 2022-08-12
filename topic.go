package redimq

type Topic struct {
	Name string
	MQClient
}

func (t *Topic) PublishMessage(m Message) error {
	return nil
}