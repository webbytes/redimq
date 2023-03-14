package redimq

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Topic struct {
	StreamKey              string
	Name                   string
	Retention              *time.Duration
	MaxLen                 *int64
	MaxIdleTimeForMessages time.Duration
	NeedsAcknowledgements  bool
	MQClient
}

func (t *Topic) getMinId() string {
	return fmt.Sprint(time.Now().Add(-*t.Retention).UnixMilli())
}

// / execute XADD queue:messages:MESSAGE_KEY MAXLEN ~ 10000 * <...data>
func (t *Topic) PublishMessage(m *Message) error {
	args := &redis.XAddArgs{
		Stream: t.StreamKey,
		Values: m.Data,
		ID:     "*",
	}
	if t.Retention != nil {
		args.MinID = t.getMinId()
		args.Approx = true
	}
	if t.MaxLen != nil {
		args.Approx = true
		args.MaxLen = *t.MaxLen
	}
	res, err := t.MQClient.rc.XAdd(t.MQClient.c, args).Result()
	m.Id = res
	m.Topic = *t
	return err
}

func (t *Topic) ConsumeMessages(consumerGroupName string, consumerName string, count int64) ([]*Message, error) {
	res, err := claimStuckStreamMessages(t.MQClient, consumerGroupName, consumerName, count, t.StreamKey, t.MaxIdleTimeForMessages)
	if err != nil {
		println("claim stuck message error - ", err.Error())
		//return nil, err
	}
	msgs := xMessageArrayToMessageArray(res, *t, consumerGroupName, consumerName)
	remainingCount := count - int64(len(res))
	if remainingCount > 0 {
		res, err = readNewMessageFromStream(t.MQClient, consumerGroupName, consumerName, remainingCount, t.StreamKey)
		if err != nil {
			println("read new messages error - ", err.Error())
			return msgs, err
		}
		msgs = append(msgs, xMessageArrayToMessageArray(res, *t, consumerGroupName, consumerName)...)
	}
	return msgs, err
}
