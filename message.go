package redimq

type Message struct {
	Id string
	GroupKey string
	Data map[string]interface{}
	ConsumerGroupName string
	ConsumerName string
	Topic
}

func (m *Message) SendToDeadLetter() error {
	return nil
}

func (m *Message) SendBackToMQ() error {
	return nil
}
func (m *Message) Acknowledge() error {
	_, err := m.Topic.MQClient.rc.XAck(m.Topic.MQClient.c, m.Topic.StreamKey, m.ConsumerGroupName, m.Id).Result()
	return err
}