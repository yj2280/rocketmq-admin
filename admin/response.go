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
	"github.com/slh92/rocketmq-admin/internal"
	"github.com/slh92/rocketmq-admin/internal/remote"
	"github.com/slh92/rocketmq-admin/primitive"
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
	return r.FromJson(jsonStr, classOfT)
}

func (r *RemotingSerializable) FromJson(jsonStr string, classOfT interface{}) (interface{}, error) {
	err := json.Unmarshal([]byte(jsonStr), classOfT)
	if err != nil {
		return nil, err
	}
	return classOfT, nil
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
	BrokerAddrTable  map[string]*internal.BrokerData
	ClusterAddrTable map[string][]string
	RemotingSerializable
}

type ConsumeStats struct {
	OffsetTable map[*primitive.MessageQueue]*OffsetWrapper
	ConsumeTps  float64
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
