package admin

import (
	"encoding/json"
	"fmt"
	"github.com/slh92/rocketmq-admin/primitive"
	"regexp"
	"strings"
)

type RemotingSerializable struct {
	ObjToKey bool `json:"-"`
}

func (r *RemotingSerializable) Encode(obj interface{}) ([]byte, error) {
	jsonStr := r.ToJson(obj, false)
	if jsonStr != "" {
		return []byte(jsonStr), nil
	}
	return nil, nil
}

func (r *RemotingSerializable) ToJson(obj interface{}, prettyFormat bool) string {
	if prettyFormat {
		jsonBytes, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	} else {
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	}
}
func (r *RemotingSerializable) Decode(data []byte, classOfT interface{}) (interface{}, error) {
	jsonStr := string(data)
	fmt.Println(jsonStr)
	return r.FromJson(jsonStr, classOfT)
}

func (r *RemotingSerializable) FromJson(jsonStr string, classOfT interface{}) (interface{}, error) {
	jsonStr, err := r.updateNoValidJson(jsonStr)
	if err != nil {
		return nil, err
	}
	jsonStr, err = r.strObjectJson(jsonStr)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(jsonStr), classOfT)
	if err != nil {
		return nil, err
	}
	return classOfT, nil
}

func (r *RemotingSerializable) updateNoValidJson(jsonStr string) (string, error) {
	// 匹配不符合JSON规范的键
	re := regexp.MustCompile(`(?m)(([{,]\s*)(\d+):)`)

	// 替换不符合规范的键为带双引号的键
	validJSONStr := re.ReplaceAllStringFunc(jsonStr, func(match string) string {
		// 取出匹配的键
		re2 := regexp.MustCompile(`(?m)(\d+)`)
		match2 := re2.ReplaceAllStringFunc(match, func(key string) string {
			return fmt.Sprintf(`"%s"`, key)
		})
		//key=strings.TrimPrefix(key,",")
		return match2
	})
	return validJSONStr, nil
}

func (r *RemotingSerializable) strObjectJson(jsonStr string) (string, error) {
	// 匹配不符合JSON规范的键
	re := regexp.MustCompile(`(?m)(([{,]\s*)(\{[^{}]*\}):)`)
	r.ObjToKey = re.Match([]byte(jsonStr))
	if !r.ObjToKey {
		return jsonStr, nil
	}

	// 替换不符合规范的键为带双引号的键
	validJSONStr := re.ReplaceAllStringFunc(jsonStr, func(match string) string {
		// 取出匹配的键
		re2 := regexp.MustCompile(`(?m)(\{[^{}]*\})`)
		match2 := re2.ReplaceAllStringFunc(match, func(key string) string {
			key = strings.ReplaceAll(key, "\"", "\\\"")
			return fmt.Sprintf(`"%s"`, key)
		})
		return match2
	})
	return validJSONStr, nil
}

type ResetOffsetRequest struct {
	ConsumerGroupList []string `json:"consumerGroupList"`
	Topic             string   `json:"topic"`
	ResetTime         int64    `json:"resetTime"`
	Force             bool     `json:"force"`
}

type ConsumerGroupRollBackStat struct {
	Status            bool             `json:"status"`
	ErrMsg            string           `json:"errMsg"`
	RollbackStatsList []*RollbackStats `json:"rollbackStatsList"`
}

type RollbackStats struct {
	BrokerName      string `json:"brokerName"`
	QueueId         int    `json:"queueId"`
	BrokerOffset    int64  `json:"brokerOffset"`
	ConsumerOffset  int64  `json:"consumerOffset"`
	TimestampOffset int64  `json:"timestampOffset"`
	RollbackOffset  int64  `json:"rollbackOffset"`
}

type GetMaxOffsetResponseHeader struct {
	Offset int64 `json:"offset"`
	RemotingSerializable
}

type TopicStatsTable struct {
	OffsetTable map[*primitive.MessageQueue]*TopicOffset `json:"offsetTable"`
	RemotingSerializable
}

type TopicOffset struct {
	MinOffset           int64 `json:"minOffset"`
	MaxOffset           int64 `json:"maxOffset"`
	LastUpdateTimestamp int64 `json:"lastUpdateTimestamp"`
}

type SendTopicMessageRequest struct {
	Topic       string `json:"topic"`
	Key         string `json:"key"`
	Tag         string `json:"tag"`
	MessageBody string `json:"messageBody"`
}

type DeleteSubGroupRequest struct {
	GroupName      string   `json:"groupName"`
	BrokerNameList []string `json:"brokerNameList"`
}

type ConsumerConfigInfo struct {
	ClusterNameList         []string                `json:"clusterNameList"`
	BrokerNameList          []string                `json:"brokerNameList"`
	SubscriptionGroupConfig SubscriptionGroupConfig `json:"subscriptionGroupConfig"`
}

type ConnectionInfo struct {
	*Connection
	VersionDesc string `json:"versionDesc"`
}

type ConsumerConnectionInfo struct {
	ConnectionSet []ConnectionInfo `json:"connectionSet"`
	*ConsumerConnection
}

type GroupList struct {
	GroupList []string `json:"groupList"`
	RemotingSerializable
}

type MessageView primitive.MessageExt

type MessageTrack struct {
	ConsumerGroup string `json:"consumerGroup"`
	ExceptionDesc string `json:"exceptionDesc"`
	TrackType     string `json:"trackType"`
}

func (m *MessageView) CompareTo(o interface{}) int {
	old := o.(*MessageView)
	return int(m.StoreTimestamp - old.StoreTimestamp)
}

type QueryResult struct {
	IndexLastUpdateTimestamp int64          `json:"indexLastUpdateTimestamp"`
	MessageList              []*MessageView `json:"messageList"`
}

type Comparable interface {
	CompareTo(o interface{}) int
}
