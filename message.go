package redimq

type Message struct {

}

func (m *Message) SendToDeadLetter() error {
	return nil
}

func (m *Message) SendBackToMQ() error {
	return nil
}