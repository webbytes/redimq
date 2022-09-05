package redimq

import (
	"math/rand"
	"testing"
	"time"
)

func TestClientNewTopic(t *testing.T) {
	// mock.ExpectSAdd("redimq:" + string(UngroupedMessages), "test").SetVal(1)
	topic, err := client.NewTopic("test", nil)
	if err != nil {
		t.Error("Topic creation returned error", err)
	}
	if topic == nil {
		t.Error("Topic creation failed")
	}
	if topic.Name != "test" {
		t.Error("Topic name not set correctly")
	}
}

func TestClientNewTopicWithOptions(t *testing.T) {
	// mock.ExpectSAdd("redimq:" + string(UngroupedMessages), "test").SetVal(1)
	duration := "1h" 
	idle := "5m"
	topic, err := client.NewTopic("test", &TopicOptions{MaxRetentionDuration: &duration, MaxIdleTimeForMessages: &idle })
	if err != nil {
		t.Error("Topic creation returned error", err)
	}
	if topic == nil {
		t.Error("Topic creation failed")
	}
	if topic.Name != "test" {
		t.Error("Topic name not set correctly")
	}
}

func TestClientNewGroupedMessageTopic(t *testing.T) {
	// mock.ExpectSAdd("redimq:" + string(GroupedMessages), "test").SetVal(1)
	topic, err := client.NewGroupedMessageTopic("test", nil)
	if err != nil {
		t.Error("GroupedMessageTopic creation returned error", err)
	}
	if topic == nil {
		t.Error("GroupedMessageTopic creation failed")
	}
	if topic.Name != "test" {
		t.Error("Topic name not set correctly")
	}
}

func TestClientNewGroupedMessageTopicWithOptions(t *testing.T) {
	// mock.ExpectSAdd("redimq:" + string(GroupedMessages), "test").SetVal(1)
	duration := "1h" 
	idle := "5m"
	topic, err := client.NewGroupedMessageTopic("test", &TopicOptions{MaxRetentionDuration: &duration, MaxIdleTimeForMessages: &idle})
	if err != nil {
		t.Error("GroupedMessageTopic creation returned error", err)
	}
	if topic == nil {
		t.Error("GroupedMessageTopic creation failed")
	}
	if topic.Name != "test" {
		t.Error("Topic name not set correctly")
	}
}


func TestGetAllUngroupedMessageTopics(t *testing.T) {
	result := []string{"test"}
	// mock.ExpectSMembers("redimq:" + string(UngroupedMessages)).SetVal(result)
	topics, err := client.GetAllUngroupedMessageTopics()
	if err != nil {
		t.Error("GetAllTopics returned error", err)
	}
	if topics == nil {
		t.Error("GetAllTopics failed")
	}
	if len(topics) != len(result) {
		t.Error("GetAllTopics result count does not match")
	}
}

func TestFindUngroupedMessageTopics(t *testing.T) {
	var cursor uint64 = 0
	var count int64 = 1
	var topics []*Topic
	pattern := "*"
	var err error
	// result := []string{"test1","test2","test3","test4"}
	rand.Seed(time.Now().Unix())
	// cursors := rand.Perm(len(result))
	i := 0
	for {
		// mock.ExpectSScan("redimq:" + string(UngroupedMessages), cursor, pattern, count).SetVal(result[i:i+int(count)], uint64(cursors[i]))
		i = i + int(count)
		topics, cursor, err = client.FindUngroupedMessageTopics(&pattern, count, cursor)
		if err != nil {
			t.Error("ScanTopics returned error", err)
		}
		if topics == nil {
			t.Error("ScanTopics failed")
		}
		if cursor == 0 {
			break
		} else {
			if int64(len(topics)) != count {
				t.Error("ScanTopics count does not match")
			}
		}
	}
}

func TestGetGroupedMessageTopics(t *testing.T) {
	result := []string{"test"}
	// mock.ExpectSMembers("redimq:" + GroupedMessages).SetVal(result)
	topics, err := client.GetAllGroupedMessageTopics()
	if err != nil {
		t.Error("GetAllGroupedMessageTopics returned error", err)
	}
	if topics == nil {
		t.Error("GetAllGroupedMessageTopics failed")
	}
	if len(topics) != len(result) {
		t.Error("GetAllGroupedMessageTopics result count does not match")
	}
}


func TestFindGroupedMessageTopics(t *testing.T) {
	var cursor uint64 = 0
	var count int64 = 1
	var topics []*GroupedMessageTopic
	pattern := "*"
	var err error
	// result := []string{"test1","test2","test3","test4"}
	rand.Seed(time.Now().Unix())
	// cursors := rand.Perm(len(result))
	i := 0
	for {
		// mock.ExpectSScan("redimq:" + GroupedMessages, cursor, pattern, count).SetVal(result[i:i+int(count)], uint64(cursors[i]))
		i = i + int(count)
		topics, cursor, err = client.FindGroupedMessageTopics(&pattern, count, cursor)
		if err != nil {
			t.Error("ScanTopics returned error", err)
		}
		if topics == nil {
			t.Error("ScanTopics failed")
		}
		if cursor == 0 {
			break
		} else {
			if int64(len(topics)) != count {
				t.Error("ScanTopics count does not match")
			}
		}
	}
}