package redimq

import (
	"time"
)

// GroupedMessageTopic is used for interacting with Grouped Message Topics. It is created using
// the NewGroupedMessageTopic function of the MQClient. It allows consuming one msg at a time to
// maintain the order.
//
// #Example for publishing messages:
//	
// 	func Main() {
//		gmt, err := client.NewGroupedMessageTopic("testQueue")
// 		if err == nil {
//			gmt.PublishMessage(message)
//		}
//	}
//
// #Example for consuming messages in a loop:
//	
// 	func Main() {
//		gmt, err := client.NewGroupedMessageTopic("testQueue")
// 		if err != nil {
//			for {
//				msgGroup, err := gmt.GetAndLockMessageGroup(consumerGroupName, consumerName)
//				if err == nil && msgGroup != nil {
//					for { // Loop till you do not get a msg back
//						msg, err := msgGroup.ConsumeMessage()
//						if err != nil && msg == nil {
//							break;
//						}
//					}
//				}
//			}
//		}
//	}
//
type GroupedMessageTopic struct {
	MessageGroupStreamKey string
	Name string
	Retention *time.Duration
	MaxLen *int64
	MaxIdleTimeForMessages time.Duration
	MaxIdleTimeForMessageGroups time.Duration
	NeedsAcknowledgements bool
	MQClient
}

func (t *GroupedMessageTopic) PublishMessage(groupKey string, m Message) error {
	return nil
}

// GetAndLockMessageGroup is used to get a lock on any available Message Group. The instance of
// [MessageGroup] returned will be used to consume messages for that specific Message Group.
func (t *GroupedMessageTopic) GetAndLockMessageGroup(consumerGroupName string, consumerName string) (*MessageGroup, error) {
	res, err := readNewStreamMessages(t.MQClient, consumerGroupName, consumerName, 1, t.MessageGroupStreamKey)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		res, err = claimStuckStreamMessages(t.MQClient, consumerGroupName, consumerName, 1, t.MessageGroupStreamKey, t.MaxIdleTimeForMessageGroups)
	}
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	key := res[0].Values["key"].(string)
	id := res[0].ID
	topic := &Topic{
		StreamKey: "redismq:gmts:" + t.Name + ":messages:" + key,
		Name: t.Name + ":" + key,
		Retention: t.Retention,
		MaxLen: t.MaxLen,
		MaxIdleTimeForMessages: t.MaxIdleTimeForMessages,
		NeedsAcknowledgements: t.NeedsAcknowledgements,
		MQClient: t.MQClient,
	}
	mg := &MessageGroup{
		Id: id,
		Key: key,
		ConsumerGroupName: consumerGroupName,
		ConsumerName: consumerName,
		Topic: *topic,
	}
	return mg, err
}
