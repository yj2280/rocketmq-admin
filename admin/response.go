/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package admin

import (
	"encoding/json"
	"fmt"
	"github.com/slh92/rocketmq-admin/internal"
	"github.com/slh92/rocketmq-admin/internal/remote"
	"github.com/slh92/rocketmq-admin/primitive"
	"math"
	"regexp"
	"strings"
)

type ConsumeType string
type MessageModel string
type ConsumeFromWhere string

const (
	CONSUME_ACTIVELY                                      = ConsumeType("PULL")
	CONSUME_PASSIVELY                                     = ConsumeType("PUSH")
	BROADCASTING                                          = MessageModel("BROADCASTING")
	CLUSTERING                                            = MessageModel("CLUSTERING")
	CONSUME_FROM_LAST_OFFSET                              = ConsumeFromWhere("CONSUME_FROM_LAST_OFFSET")
	CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST = ConsumeFromWhere("CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST")
	CONSUME_FROM_MIN_OFFSET                               = ConsumeFromWhere("CONSUME_FROM_MIN_OFFSET")
	CONSUME_FROM_MAX_OFFSET                               = ConsumeFromWhere("CONSUME_FROM_MAX_OFFSET")
	CONSUME_FROM_FIRST_OFFSET                             = ConsumeFromWhere("CONSUME_FROM_FIRST_OFFSET")
	CONSUME_FROM_TIMESTAMP                                = ConsumeFromWhere("CONSUME_FROM_TIMESTAMP")
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

type TopicList struct {
	TopicList  []string
	BrokerAddr string
	RemotingSerializable
}

type ConsumerConnection struct {
	ConnectionSet     []Connection
	SubscriptionTable map[string]SubscriptionData
	ConsumeType       ConsumeType
	MessageModel      MessageModel
	ConsumeFromWhere  ConsumeFromWhere
	RemotingSerializable
}

func (c *ConsumerConnection) ComputeMinVersion() int64 {
	minVersion := int64(2147483647)

	for _, conn := range c.ConnectionSet {
		if conn.Version < minVersion {
			minVersion = conn.Version
		}
	}
	return minVersion
}

type Connection struct {
	ClientId   string
	ClientAddr string
	Language   remote.LanguageCode
	Version    int64
}

type SubscriptionData struct {
	ClassFilterMode   bool
	Topic             string
	SubString         string
	TagsSet           []string
	CodeSet           []int
	SubVersion        int64
	FilterClassSource string
}

type SubscriptionGroupWrapper struct {
	SubscriptionGroupTable map[string]SubscriptionGroupConfig
	DataVersion            DataVersion
	RemotingSerializable
}

type DataVersion struct {
	Timestamp int64
	Counter   int32
}

type SubscriptionGroupConfig struct {
	GroupName                      string
	ConsumeEnable                  bool
	ConsumeFromMinEnable           bool
	ConsumeBroadcastEnable         bool
	RetryMaxTimes                  int
	RetryQueueNums                 int
	BrokerId                       int
	WhichBrokerWhenConsumeSlowly   int
	NotifyConsumerIdsChangedEnable bool
}

type GroupConsumeInfo struct {
	Group        string
	Version      string
	Count        int
	ConsumeType  ConsumeType
	MessageModel MessageModel
	ConsumeTps   int
	DiffTotal    int64
}

type ClusterInfo struct {
	BrokerAddrTable  map[string]*internal.BrokerData `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string             `json:"clusterAddrTable"`
	RemotingSerializable
}

type ConsumeStats struct {
	OffsetTable map[*primitive.MessageQueue]*OffsetWrapper `json:"-"`
	ConsumeTps  float64                                    `json:"consumeTps"`
	RemotingSerializable
}

func (c *ConsumeStats) ComputeTotalDiff() int64 {
	diffTotal := int64(0)
	for _, wrapper := range c.OffsetTable {
		diff := wrapper.BrokerOffset - wrapper.ConsumerOffset
		diffTotal += diff
	}
	return diffTotal
}

type OffsetWrapper struct {
	BrokerOffset   int64
	ConsumerOffset int64
	LastTimestamp  int64
}

type TopicConsumerInfo struct {
	Topic             string
	DiffTotal         int64
	LastTimestamp     int64
	QueueStatInfoList []*QueueStatInfo
}

func (t *TopicConsumerInfo) AppendQueueStatInfo(queueStat *QueueStatInfo) {
	t.QueueStatInfoList = append(t.QueueStatInfoList, queueStat)
	t.DiffTotal = t.DiffTotal + (queueStat.BrokerOffset - queueStat.ConsumerOffset)
	t.LastTimestamp = int64(math.Max(float64(t.LastTimestamp), float64(queueStat.LastTimestamp)))
}

type QueueStatInfo struct {
	BrokerName     string
	QueueId        int
	BrokerOffset   int64
	ConsumerOffset int64
	ClientInfo     string
	LastTimestamp  int64
}

func (q *QueueStatInfo) FromOffsetTableEntry(key *primitive.MessageQueue, value *OffsetWrapper) {
	q.BrokerName = key.BrokerName
	q.QueueId = key.QueueId
	q.BrokerOffset = value.BrokerOffset
	q.ConsumerOffset = value.ConsumerOffset
	q.LastTimestamp = value.LastTimestamp
}

type AdminConsumerRunningInfo struct {
	*internal.ConsumerRunningInfo
	RemotingSerializable
}
