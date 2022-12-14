package redimq

import (
	"fmt"
	"math"
	"time"

	"github.com/go-redis/redis/v8"
)

// GroupedMessageTopic is used for interacting with Grouped Message Topics. It is created using
// the NewGroupedMessageTopic function of the MQClient. It allows consuming one msg at a time to
// maintain the order.
//
// #Example for publishing messages:
//
//	func Main() {
//		gmt, err := client.NewGroupedMessageTopic("testQueue", options)
//		if err == nil {
//			gmt.PublishMessage(message)
//		}
//	}
//
// #Example for consuming messages in a loop:
//
//	func Main() {
//		gmt, err := client.NewGroupedMessageTopic("testQueue", options)
//		if err != nil {
//			res, err := gmt.InitializeConsumer(consumerGroupName, consumerName)
//			for {
//				msgs, err := gmt.ConsumeMessages(consumerGroupName, consumerName)
//				if err == nil && len(msgs) > 0 {
//					for _,msg := range msgs { // Loop to process each message
//						// TODO to process each message
//					}
//				}
//			}
//		}
//	}
type GroupedMessageTopic struct {
	MessageGroupStreamKey    string // {redimq:gmts:test}:messagegroups
	MessageGroupSetKey       string // {redimq:gmts:test}:messagegroupset
	StreamPrefix             string // redimq:gmts:test
	MessageCountKey          string // redimq:gmts:test:messagecount
	Name                     string
	Retention                *time.Duration
	MaxIdleTimeForMessages   time.Duration
	RebalanceInterval        time.Duration
	NeedsAcknowledgements    bool
	MessageKeysBeingConsumed []string
	MQClient
}

func (t *GroupedMessageTopic) getStreamKeyForGroup(groupKey string) string {
	return t.StreamPrefix + ":mg:" + groupKey + ":messages"
}

// PublishMessage is used to publish any message to the GroupedMessageTopic. The message group key
// is some string that you want to group your messages by. Messages in the same message group will
// be consumed in sequence. Messages in different message groups need not be process in order. The
// differnt message groups are there to provide some parallelism in the consumers. If a single message
// group key is used for all messages, then this would ensure order of processing, But this would also
// lead to having only one effective consumer at a time. There is no current limit on the number of
// message groups keys that you can have. The messages follow the retention defined during the queue
// creation.
func (t *GroupedMessageTopic) PublishMessage(groupKey string, m *Message) error {
	rc := t.MQClient.rc
	c := t.MQClient.c
	topic := &Topic{
		StreamKey:              t.getStreamKeyForGroup(groupKey),
		Retention:              t.Retention,
		NeedsAcknowledgements:  t.NeedsAcknowledgements,
		MaxIdleTimeForMessages: t.MaxIdleTimeForMessages,
		MQClient:               t.MQClient,
	}
	arr := []interface{}{groupKey}
	_, err := RediMQScripts.PublishToGMT.Run(c, rc, []string{topic.StreamKey, t.MessageGroupStreamKey, t.MessageCountKey}, arr).Result()
	if err != nil {
		return err
	}
	res, err := rc.XAdd(c, &redis.XAddArgs{
		Stream: topic.StreamKey,
		Values: m.Data,
		ID:     "*",
	}).Result()
	m.Id = res
	m.Topic = *topic
	return err
}

func (t *GroupedMessageTopic) getTopic() *Topic {
	return &Topic{
		StreamKey:              t.MessageGroupStreamKey,
		Name:                   t.Name,
		Retention:              t.Retention,
		MaxIdleTimeForMessages: t.MaxIdleTimeForMessages,
		MQClient:               t.MQClient,
	}
}

func (t *GroupedMessageTopic) InitializeConsumer(consumerGroupName string, consumerName string) (string, error) {
	gres, err := t.MQClient.rc.XGroupCreateMkStream(t.MQClient.c, t.MessageGroupStreamKey, consumerGroupName, "0").Result()
	cres, err := t.MQClient.rc.XGroupCreateConsumer(t.MQClient.c, t.MessageGroupStreamKey, consumerGroupName, consumerName).Result()
	return fmt.Sprintf("%s, %d", gres, cres), err
}

func (t *GroupedMessageTopic) lockMessageGroups(consumerGroupName string, consumerName string) ([]*Message, error) {
	count := t.getGroupCountPerConsumer(t.Name, consumerGroupName, consumerName, t.MessageGroupStreamKey)
	res, err := reclaimMessageGroup(t.MQClient, consumerGroupName, consumerName, count, t.MessageGroupStreamKey)
	if err != nil {
		fmt.Print("Error reclaiming message groups: ", err)
		return nil, err
	}
	mgs := xMessageArrayToMessageArray(res, *t.getTopic(), consumerGroupName, consumerName)
	lessCount := count - int64(len(mgs))
	if lessCount > 0 {
		res, err = readNewMessageFromStream(t.MQClient, consumerGroupName, consumerName, count, t.MessageGroupStreamKey)
		if err != nil {
			fmt.Print("Error reading new message groups: ", err)
			return nil, err
		}
		mgs = append(mgs, xMessageArrayToMessageArray(res, *t.getTopic(), consumerGroupName, consumerName)...)
		lessCount = count - int64(len(mgs))
		if lessCount > 0 {
			res, err = claimStuckStreamMessages(t.MQClient, consumerGroupName, consumerName, lessCount, t.MessageGroupStreamKey, t.MaxIdleTimeForMessages)
			if err != nil {
				fmt.Print("Error claiming stuck message groups: ", err)
				return nil, err
			}
			mgs = append(mgs, xMessageArrayToMessageArray(res, *t.getTopic(), consumerGroupName, consumerName)...)
		}
	}
	// fmt.Printf("Group: %s, Consumer: %s, Message Group Locks Requested: %d, Message Groups Locked: %d",
	// 	consumerGroupName, consumerName, count, len(mgs))
	return mgs, err
}

// ConsumeMessages is used to consume messages from the GroupedMessageTopic. This function will try obtaining a
// lock on the a fixed number of message groups (say N) and then consume 1 message from each group locked. The
// number of message group locks requested (N) depends on the total number of message groups present (MG) and
// the total number of consumers (C) for the consumer group (N = MG / C + 1). The function would return one
// message from each message group locked and having messages. So it can return a maximum of N messages and a
// minimum  of 0 messages if none of the message groups have any messages.
func (t *GroupedMessageTopic) ConsumeMessages(consumerGroupName string, consumerName string) ([]*Message, error) {
	mgs, err := t.lockMessageGroups(consumerGroupName, consumerName)
	var msgs []*Message
	for _, g := range mgs {
		t.MQClient.rc.XGroupCreate(t.MQClient.c, t.getStreamKeyForGroup(g.GroupKey), consumerGroupName, "0").Result()
		t.MQClient.rc.XGroupCreateConsumer(t.MQClient.c, t.getStreamKeyForGroup(g.GroupKey), consumerGroupName, consumerName).Result()
		res, err := claimStuckStreamMessages(t.MQClient, consumerGroupName, consumerName, 1, t.getStreamKeyForGroup(g.GroupKey), t.MaxIdleTimeForMessages)
		if err != nil {
			fmt.Print("Error claiming stuck messages for "+g.GroupKey+": ", err)
		} else if len(res) == 0 {
			res, err = readNewMessageFromStream(t.MQClient, consumerGroupName, consumerName, 1, t.getStreamKeyForGroup(g.GroupKey))
			if err != nil {
				fmt.Print("Error reading new message for "+g.GroupKey+": ", err)
			}
		}
		if err == nil && res != nil && len(res) > 0 {
			topic := &Topic{
				StreamKey:              t.getStreamKeyForGroup(g.GroupKey),
				Name:                   "",
				Retention:              t.Retention,
				MaxIdleTimeForMessages: t.MaxIdleTimeForMessages,
				NeedsAcknowledgements:  t.NeedsAcknowledgements,
				MQClient:               t.MQClient,
			}
			msgs = append(msgs, xMessageArrayToMessageArray(res, *topic, consumerGroupName, consumerName)...)
		}
	}
	// fmt.Printf("Group: %s, Consumer: %s, Messages Pulled: %d\n",
	// 	consumerGroupName, consumerName, len(msgs))
	return msgs, err
}

// ConsumeMessages is used to consume messages from the GroupedMessageTopic. This function will try obtaining a
// lock on the a fixed number of message groups (say N) and then consume 1 message from each group locked. The
// number of message group locks requested (N) depends on the total number of message groups present (MG) and
// the total number of consumers (C) for the consumer group (N = MG / C + 1). The function would return one
// message from each message group locked and having messages. So it can return a maximum of N messages and a
// minimum  of 0 messages if none of the message groups have any messages.
func (t *GroupedMessageTopic) ConsumeMessagesInBatches(consumerGroupName string, consumerName string, batchSize int64) ([][]*Message, error) {
	mgs, err := t.lockMessageGroups(consumerGroupName, consumerName)
	msgs := [][]*Message{}
	streamKeys := []string{}
	for _, g := range mgs {
		t.MQClient.rc.XGroupCreate(t.MQClient.c, t.getStreamKeyForGroup(g.GroupKey), consumerGroupName, "0").Result()
		res, err := claimStuckStreamMessages(t.MQClient, consumerGroupName, consumerName, batchSize, t.getStreamKeyForGroup(g.GroupKey), t.MaxIdleTimeForMessages)
		if err == nil && res != nil && len(res) > 0 {
			topic := &Topic{
				StreamKey:              t.getStreamKeyForGroup(g.GroupKey),
				Name:                   "",
				Retention:              t.Retention,
				MaxIdleTimeForMessages: t.MaxIdleTimeForMessages,
				NeedsAcknowledgements:  t.NeedsAcknowledgements,
				MQClient:               t.MQClient,
			}
			msgs = append(msgs, xMessageArrayToMessageArray(res, *topic, consumerGroupName, consumerName))
		} else {
			streamKeys = append(streamKeys, t.getStreamKeyForGroup(g.GroupKey))
		}
	}
	if len(streamKeys) > 0 {
		streamMsgs, err := readNewStreamMessages(t.MQClient, consumerGroupName, consumerName, batchSize, streamKeys)
		if err == nil {
			for _, m := range streamMsgs {
				topic := &Topic{
					StreamKey:              m.Stream,
					Name:                   "",
					Retention:              t.Retention,
					MaxIdleTimeForMessages: t.MaxIdleTimeForMessages,
					NeedsAcknowledgements:  t.NeedsAcknowledgements,
					MQClient:               t.MQClient,
				}
				msgs = append(msgs, xMessageArrayToMessageArray(m.Messages, *topic, consumerGroupName, consumerName))
			}
		}
	}
	fmt.Printf("Group: %s, Consumer: %s, Message Batches Pulled: %d\n",
		consumerGroupName, consumerName, len(msgs))
	return msgs, err
}

func (t *GroupedMessageTopic) CleanupOfTopicAndMessageGroups() {
	start := "-"
	for {
		res, err := t.MQClient.rc.XRange(t.MQClient.c, t.MessageGroupStreamKey, start, "+").Result()
		if err != nil {
			println("Iterating message groups error - ", err.Error())
		}
		if len(res) == 0 {
			return
		}
		for _, m := range res {
			_, err = RediMQScripts.DeleteMessageGroupIfEmpty.Run(t.MQClient.c, t.MQClient.rc,
				[]string{t.MessageGroupStreamKey, t.getStreamKeyForGroup(m.Values["key"].(string))},
				[]interface{}{m.ID},
			).Result()
			if err != nil {
				println("Deleting Messge Group error - ", err.Error())
			}
			start = "(" + m.ID
		}
	}
}

func (t *GroupedMessageTopic) RebalanceConsumers(consumerGroupName string, consumerName string) {
	res, err := t.MQClient.rc.SetNX(t.MQClient.c, t.StreamPrefix+":"+consumerGroupName+":rebalancer", consumerName, t.RebalanceInterval).Result()
	if err != nil {
		fmt.Print("Error trying to get rebalance lock: ", err)
	}
	if res {
		t.CleanupOfTopicAndMessageGroups()
		consumerCount := t.getActiveConsumersForConsumerGroup(t.MessageGroupStreamKey, consumerGroupName, consumerName)
		groupCount, _ := t.MQClient.rc.XLen(t.MQClient.c, t.MessageGroupStreamKey).Result()
		groupsPerConsumer := int64(math.Ceil(float64(groupCount) / float64(consumerCount)))
		_, err := t.MQClient.rc.Set(t.MQClient.c, t.StreamPrefix+":"+consumerGroupName+":groups-per-consumer", groupsPerConsumer, 0).Result()
		if err != nil {
			fmt.Print("Error trying to get rebalance lock: ", err)
		}
	}
}

func (t *GroupedMessageTopic) getActiveConsumersForConsumerGroup(stream string, consumerGroupName string, consumerName string) int {
	count := 1
	res, err := t.MQClient.rc.XInfoConsumers(t.MQClient.c, stream, consumerGroupName).Result()
	if err != nil {
		fmt.Print("xinfo consumers error - ", err)
	}
	for _, c := range res {
		if c.Name != consumerName && c.Idle < int64(t.MaxIdleTimeForMessages/time.Millisecond) {
			count = count + 1
		}
	}
	return count
}

func (t *GroupedMessageTopic) getGroupCountPerConsumer(topicName string, consumerGroupName string, consumerName string, messageGroupKey string) int64 {
	consumerCount := t.getActiveConsumersForConsumerGroup(messageGroupKey, consumerGroupName, consumerName)
	res, _ := t.MQClient.rc.XLen(t.MQClient.c, messageGroupKey).Result()
	return int64(math.Ceil(float64(res) / float64(consumerCount)))
}
