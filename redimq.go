// RediMQ is a library to enable and simplfy using REDIS as a Message Queue. The library
// is built on the [go-redis/redis] module (v8 and above) for working with the REDIS backend.
// RediMQ Supports two types of queues:
//
// 1. Ungrouped Message Topic (umts) - Unordered queue having multiple consumer groups 
//    and each consumer group with multiple consumers. This can be done using the [Topic] struct.
// 2. Grouped Message Topic (gmts) - Queue which maintains order at a message group level. 
//    The Queue allows multiple consumer groups each having multiple consumers. This can be done
//    using the [GroupedMessageTopic] struct.
//
// The library leverages the REDIS Streams internally to provide the message queue features.
// 
// [Documentation] : https://github.com/webbytes/redimq#redimq
//
package redimq // import "github.com/webbytes/redimq"

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)


// System Defaults for the MQClient and Topics
var (
// DefaultMaxIdleTimeForMessage defines the maximum duration for which a specific message
// would be considered as locked. After this duration, if it is not acknowledge, the message 
// would be considered a candidate for recaliming by any consumer instance. This ensures the
// messages are not lost if after a consumer has picked it up and is not able to complete the
// processing
//
// The value is a string and should be parsable by the [time.ParseDuration] function
	DefaultMaxIdleTimeForMessage string = "5m" // Default "5m" - (5 minutes)
)

// NewMQClient is used to get an instance of the MQClient object that can be used
// to work with the queues. It accepts an instance of context and a REDIS client
func NewMQClient(c context.Context, rc *redis.Client) (*MQClient, error) {
	client := &MQClient{ rc: rc, c: c }
	err := initializeRediMQ(c, rc)
	return client, err
}

func initializeRediMQ(c context.Context, rc *redis.Client) error {
	initializeScripts()
	_,err := rc.Ping(c).Result()
	if err != nil {
		return fmt.Errorf("RediMQ initialization failed: [%w]", err)
	}
	return nil
}