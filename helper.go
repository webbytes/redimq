package redimq

import (
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

func claimStuckStreamMessages(client MQClient, consumerGroupName string, consumerName string, count int64, stream string, idle time.Duration) ([]redis.XMessage, error) {
	// args := &redis.XAutoClaimArgs{
	// 	Stream:   stream,
	// 	Group:    consumerGroupName,
	// 	Consumer: consumerName,
	// 	Count:    count,
	// 	Start:    "-",
	// 	MinIdle:  idle,
	// }
	// msgs, a, err := client.rc.XAutoClaim(client.c, args).Result()
	var msgs []redis.XMessage
	res, err := client.rc.XPendingExt(client.c, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  consumerGroupName,
		Idle:   idle,
		Start:  "-",
		End:    "+",
		Count:  count,
	}).Result()
	if err != nil {
		println("XPending", err.Error())
		return nil, err
	}
	if len(res) > 0 {
		ids := make([]string, len(res))
		for i, m := range res {
			ids[i] = m.ID
		}
		msgs, err = client.rc.XClaim(client.c, &redis.XClaimArgs{
			Stream:   stream,
			Group:    consumerGroupName,
			Consumer: consumerName,
			MinIdle:  idle,
			Messages: ids,
		}).Result()
		if err != nil {
			println("XClaim", err.Error())
			return nil, err
		}
	}
	if len(res) == 0 {
		return []redis.XMessage{}, nil
	}
	return msgs, err
}

func reclaimMessageGroup(client MQClient, consumerGroupName string, consumerName string, count int64, stream string, maxIdle time.Duration) ([]redis.XMessage, error) {
	pending, err := client.rc.XPendingExt(client.c, &redis.XPendingExtArgs{
		Stream:   stream,
		Group:    consumerGroupName,
		Start:    "-",
		End:      "+",
		Count:    count,
		Consumer: consumerName,
		Idle:     0,
	}).Result()
	if err != nil {
		return nil, err
	}
	msgIds := []string{}
	for i, p := range pending {
		if p.Idle < (maxIdle - 10*time.Second) {
			msgIds[i] = p.ID
		}
	}
	msgs, err := client.rc.XClaim(client.c, &redis.XClaimArgs{
		Stream:   stream,
		Group:    consumerGroupName,
		Consumer: consumerName,
		MinIdle:  0,
		Messages: msgIds,
	}).Result()
	if err != nil {
		return nil, err
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
