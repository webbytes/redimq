package redimq

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func readNewMessageFromStream(client MQClient, consumerGroupName string, consumerName string, count int64, stream string) ([]redis.XMessage, error) {
	args := &redis.XReadGroupArgs{
		Group:    consumerGroupName,
		Consumer: consumerName,
		Count:    count,
		Block:    -1,
		Streams:  []string{stream, ">"},
	}
	res, err := client.rc.XReadGroup(client.c, args).Result()
	if err != nil && err.Error() != "redis: nil" {
		return nil, err
	}
	if len(res) == 0 || res[0].Messages == nil {
		return []redis.XMessage{}, nil
	}
	return res[0].Messages, nil
}

func readNewStreamMessages(client MQClient, consumerGroupName string, consumerName string, count int64, streams []string) ([]redis.XStream, error) {
	streamKeys := streams
	for _, _ = range streams {
		streamKeys = append(streamKeys, ">")
	}
	args := &redis.XReadGroupArgs{
		Group:    consumerGroupName,
		Consumer: consumerName,
		Count:    count,
		Streams:  streamKeys,
	}
	res, err := client.rc.XReadGroup(client.c, args).Result()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 || res[0].Messages == nil {
		return []redis.XStream{}, nil
	}
	return res, nil
}

func claimStuckStreamMessages(client MQClient, consumerGroupName string, consumerName string, count int64, stream string, idle time.Duration) ([]redis.XMessage, error) {
	var msgs []redis.XMessage
	args := &redis.XPendingExtArgs{
		Stream: stream,
		Group:  consumerGroupName,
		Count:  count,
		Start:  "-",
		End:    "+",
		Idle:   idle,
	}
	res, err := client.rc.XPendingExt(client.c, args).Result()
	if err != nil {
		fmt.Print("XPending: ", err.Error())
		return nil, err
	}
	if len(res) > 0 {
		ids := make([]string, len(res))
		for i, m := range res {
			ids[i] = m.ID
		}
		args := &redis.XClaimArgs{
			Stream:   stream,
			Group:    consumerGroupName,
			Consumer: consumerName,
			MinIdle:  idle,
			Messages: ids,
		}
		msgs, err = client.rc.XClaim(client.c, args).Result()
		if err != nil {
			fmt.Print("XClaim: ", err)
			return nil, err
		}
	}
	if len(res) == 0 {
		return []redis.XMessage{}, nil
	}
	return msgs, err
}

func reclaimMessageGroup(client MQClient, consumerGroupName string, consumerName string, count int64, stream string) ([]redis.XMessage, error) {
	msgs := []redis.XMessage{}
	res, err := RediMQScripts.ReclaimMessageGroup.Run(client.c, client.rc,
		[]string{stream},
		[]interface{}{consumerGroupName, consumerName, count}).Result()
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.([]interface{})) == 0 {
		return msgs, nil
	}
	for _, m := range res.([]interface{}) {
		v := make(map[string]interface{})
		v["key"] = m.([]interface{})[1].([]interface{})[1]
		msg := &redis.XMessage{
			ID:     m.([]interface{})[0].(string),
			Values: v,
		}
		msgs = append(msgs, *msg)
	}
	return msgs, err
}

func xMessageToMessage(s redis.XMessage, t Topic, consumerGroupName string, consumerName string) *Message {
	groupKey := ""
	if val, ok := s.Values["key"]; ok {
		groupKey = val.(string)
	}
	return &Message{
		GroupKey:          groupKey,
		Id:                s.ID,
		Data:              s.Values,
		Topic:             t,
		ConsumerGroupName: consumerGroupName,
		ConsumerName:      consumerName,
	}
}

func xMessageArrayToMessageArray(xms []redis.XMessage, t Topic, consumerGroupName string, consumerName string) []*Message {
	msgs := make([]*Message, len(xms))
	for i, s := range xms {
		msgs[i] = xMessageToMessage(s, t, consumerGroupName, consumerName)
	}
	return msgs
}

// func xMessageToMessageGroup(s redis.XMessage, t GroupedMessageTopic, consumerGroupName string, consumerName string) *MessageGroup {
// 	topic := &Topic{
// 		StreamKey: t.getStreamKeyForGroup(s.Values["key"].(string)),
// 		Name: "",
// 		Retention: t.Retention,
// 		MaxIdleTimeForMessages: t.MaxIdleTimeForMessages,
// 		NeedsAcknowledgements: t.NeedsAcknowledgements,
// 		MQClient: t.MQClient,

// 	}
// 	return &MessageGroup {
// 		Id: s.ID,
// 		Key: s.Values["key"].(string),
// 		Topic: *topic,
// 		ConsumerGroupName: consumerGroupName,
// 		ConsumerName: consumerName,
// 	}
// }

// func xMessageArrayToMessageGroupArray(xms []redis.XMessage, t GroupedMessageTopic, consumerGroupName string, consumerName string) []*MessageGroup {
// 	msgs := make([]*MessageGroup, len(xms))
// 	for i,s := range xms {
// 		msgs[i] = xMessageToMessageGroup(s, t, consumerGroupName, consumerName)
// 	}
// 	return msgs
// }

// func xMessageArrayToMessageGroups(xms []redis.XMessage, t GroupedMessageTopic, consumerGroupName string, consumerName string) []*MessageGroups {
// 	msgs := make([]*MessageGroup, len(xms))
// 	for i,s := range xms {
// 		msgs[i] = xMessageToMessageGroup(s, t, consumerGroupName, consumerName)
// 	}
// 	return msgs
// }
