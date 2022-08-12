package redimq

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)


func NewMQClient(c context.Context, rc *redis.Client) (*MQClient, error) {
	client := &MQClient{ rc: rc, c: c }
	err := initializeRediMQ(c, rc)
	return client, err
}

func initializeRediMQ(c context.Context, rc *redis.Client) error {
	functions := []string {
		"",
		"",
	}
	_,err := rc.Ping(c).Result()
	if err != nil {
		return fmt.Errorf("RediMQ initialization failed: [%w]", err)
	}
	for _,f := range functions {
		_, err := rc.Do(c,"FCALL","LOAD", f).Result()
		if err != nil {
			return fmt.Errorf("RediMQ initialization failed: [%w]", err)
		}
	}
	return nil
}