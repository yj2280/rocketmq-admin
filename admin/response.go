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
	"github.com/slh92/rocketmq-admin/internal"
	"github.com/slh92/rocketmq-admin/internal/remote"
	"github.com/slh92/rocketmq-admin/primitive"
	"math"
	"strconv"
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

type TopicList struct {
	TopicList  []string `json:"topicList"`
	BrokerAddr string   `json:"brokerAddr"`
	RemotingSerializable
}

type ConsumerConnection struct {
	ConnectionSet     []Connection                `json:"connectionSet"`
	SubscriptionTable map[string]SubscriptionData `json:"subscriptionTable"`
	ConsumeType       ConsumeType                 `json:"consumeType"`
	MessageModel      MessageModel                `json:"messageModel"`
	ConsumeFromWhere  ConsumeFromWhere            `json:"consumeFromWhere"`
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
	ClientId   string              `json:"clientId"`
	ClientAddr string              `json:"clientAddr"`
	Language   remote.LanguageCode `json:"language"`
	Version    int64               `json:"version"`
}

type SubscriptionData struct {
	ClassFilterMode   bool     `json:"classFilterMode"`
	Topic             string   `json:"topic"`
	SubString         string   `json:"subString"`
	TagsSet           []string `json:"tagsSet"`
	CodeSet           []int    `json:"codeSet"`
	SubVersion        int64    `json:"subVersion"`
	FilterClassSource string   `json:"filterClassSource"`
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
	Group        string       `json:"group"`
	Version      string       `json:"version"`
	Count        int          `json:"count"`
	ConsumeType  ConsumeType  `json:"consumeType"`
	MessageModel MessageModel `json:"messageModel"`
	ConsumeTps   int          `json:"consumeTps"`
	DiffTotal    int64        `json:"diffTotal"`
}

func (g *GroupConsumeInfo) CompareTo(o interface{}) int {
	old := o.(*GroupConsumeInfo)
	if g.Count != old.Count {
		return old.Count - g.Count
	}
	return int(old.DiffTotal - g.DiffTotal)
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
	Topic             string           `json:"topic"`
	DiffTotal         int64            `json:"diffTotal"`
	LastTimestamp     int64            `json:"lastTimestamp"`
	QueueStatInfoList []*QueueStatInfo `json:"queueStatInfoList"`
}

func (t *TopicConsumerInfo) AppendQueueStatInfo(queueStat *QueueStatInfo) {
	t.QueueStatInfoList = append(t.QueueStatInfoList, queueStat)
	t.DiffTotal = t.DiffTotal + (queueStat.BrokerOffset - queueStat.ConsumerOffset)
	t.LastTimestamp = int64(math.Max(float64(t.LastTimestamp), float64(queueStat.LastTimestamp)))
}

type QueueStatInfo struct {
	BrokerName     string `json:"brokerName"`
	QueueId        int    `json:"queueId"`
	BrokerOffset   int64  `json:"brokerOffset"`
	ConsumerOffset int64  `json:"consumerOffset"`
	ClientInfo     string `json:"clientInfo"`
	LastTimestamp  int64  `json:"lastTimestamp"`
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

type QueryMessageResponseHeader struct {
	IndexLastUpdateTimestamp int64 `json:"indexLastUpdateTimestamp"`
	IndexLastUpdatePhyoffset int64 `json:"indexLastUpdatePhyoffset"`
}

func (request *QueryMessageResponseHeader) Decode(properties map[string]string) {
	if len(properties) == 0 {
		return
	}
	if v, existed := properties["indexLastUpdateTimestamp"]; existed {
		request.IndexLastUpdateTimestamp, _ = strconv.ParseInt(v, 32, 0)
	}

	if v, existed := properties["indexLastUpdatePhyoffset"]; existed {
		request.IndexLastUpdatePhyoffset, _ = strconv.ParseInt(v, 32, 0)
	}
}
