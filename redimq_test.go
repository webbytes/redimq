package redimq

import (
	"context"
	"os"
	"testing"

	"github.com/go-redis/redis/v8"
	// "github.com/go-redis/redismock/v8"
)

var redisClient *redis.Client
// var mock redismock.ClientMock
var client *MQClient
var topic *Topic
var gmt *GroupedMessageTopic
var clientError, topicError, gmtError error


func setupTest() {
	// redisClient, mock = redismock.NewClientMock()
	redisClient = redis.NewClient(&redis.Options{
        Addr:     "localhost:36379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })
	// mock.ExpectPing().SetVal("PONG")
	client, clientError = NewMQClient(context.TODO(), redisClient)
	topic, topicError = client.NewTopic("test", nil)
	gmt, gmtError = client.NewGroupedMessageTopic("test", nil)
}

func TestMain(m *testing.M) {
	setupTest()
	code := m.Run()
	os.Exit(code)
}

func TestRediMQNewMQClient(t *testing.T) {
	// mock.ExpectCommand("FCALL","LOAD","*")
	// if clientError != nil {
	// 	t.Error("Client creation errored", clientError)
	// }
	if client == nil {
		t.Error("Client creation failed")
	}
}