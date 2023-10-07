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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/slh92/rocketmq-admin/internal"
	"github.com/slh92/rocketmq-admin/internal/remote"
	"github.com/slh92/rocketmq-admin/internal/utils"
	"github.com/slh92/rocketmq-admin/primitive"
	"github.com/slh92/rocketmq-admin/rlog"
)

type Admin interface {
	CreateTopic(ctx context.Context, opts ...OptionCreate) error
	DeleteTopic(ctx context.Context, opts ...OptionDelete) error

	GetAllSubscriptionGroup(ctx context.Context, brokerAddr string, timeoutMillis time.Duration) (*SubscriptionGroupWrapper, error)
	FetchAllTopicList(ctx context.Context) (*TopicList, error)
	QueryTopicRouteInfo(topic string) (*internal.TopicRouteData, error)
	QueryConsumersByTopic(topic string) (*internal.TopicRouteData, error)
	QueryConsumerConnectsByGroup(ctx context.Context, group string) (*ConsumerConnection, error)
	//GetBrokerClusterInfo(ctx context.Context) (*remote.RemotingCommand, error)
	FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error)
	Close() error
}

// TODO: move outdated context to ctx
type adminOptions struct {
	internal.ClientOptions
}

type AdminOption func(options *adminOptions)

func defaultAdminOptions() *adminOptions {
	opts := &adminOptions{
		ClientOptions: internal.DefaultClientOptions(),
	}
	opts.GroupName = "TOOLS_ADMIN"
	opts.InstanceName = time.Now().String()
	return opts
}

// WithResolver nameserver resolver to fetch nameserver addr
func WithResolver(resolver primitive.NsResolver) AdminOption {
	return func(options *adminOptions) {
		options.Resolver = resolver
	}
}

func WithCredentials(c primitive.Credentials) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.Credentials = c
	}
}

// WithNamespace set the namespace of admin
func WithNamespace(namespace string) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.Namespace = namespace
	}
}

type MqAdmin struct {
	Cli internal.RMQClient

	opts *adminOptions

	closeOnce sync.Once
}

// NewAdmin initialize admin
func NewAdmin(opts ...AdminOption) (*MqAdmin, error) {
	defaultOpts := defaultAdminOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}
	namesrv, err := internal.NewNamesrv(defaultOpts.Resolver, defaultOpts.RemotingClientConfig)
	defaultOpts.Namesrv = namesrv
	if err != nil {
		return nil, err
	}

	cli := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)
	if cli == nil {
		return nil, fmt.Errorf("GetOrNewRocketMQClient faild")
	}
	defaultOpts.Namesrv = cli.GetNameSrv()
	//log.Printf("Client: %#v", namesrv.srvs)
	return &MqAdmin{
		Cli:  cli,
		opts: defaultOpts,
	}, nil
}

func (a *MqAdmin) GetAllSubscriptionGroup(ctx context.Context, brokerAddr string, timeoutMillis time.Duration) (*SubscriptionGroupWrapper, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllSubscriptionGroupConfig, nil, nil)
	response, err := a.Cli.InvokeSync(ctx, internal.BrokerVIPChannel(brokerAddr), cmd, timeoutMillis)
	if err != nil {
		rlog.Error("Get all group list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Get all group list success", map[string]interface{}{})
	}
	if response.Code != 0 {
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	var subscriptionGroupWrapper SubscriptionGroupWrapper
	_, err = subscriptionGroupWrapper.Decode(response.Body, &subscriptionGroupWrapper)
	if err != nil {
		rlog.Error("Get all group list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &subscriptionGroupWrapper, nil
}

func (a *MqAdmin) QueryGroupList() ([]*GroupConsumeInfo, error) {
	consumerGroupSet := []string{}
	clusterInfo, err := a.getBrokerClusterInfo(context.Background())
	if err != nil {
		return nil, err
	}
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		wrapper, err := a.GetAllSubscriptionGroup(context.Background(), brokerData.SelectBrokerAddr(), 3*time.Second)
		if err != nil {
			return nil, err
		}
		for key, _ := range wrapper.SubscriptionGroupTable {
			consumerGroupSet = append(consumerGroupSet, key)
		}
	}
	groupList := []*GroupConsumeInfo{}
	for _, group := range consumerGroupSet {
		res, err := a.QueryGroup(group)
		if err != nil {
			continue
		}
		groupList = append(groupList, res)
	}
	return groupList, nil
}

func (a *MqAdmin) QueryGroup(group string) (*GroupConsumeInfo, error) {
	groupConsumeInfo := &GroupConsumeInfo{}
	consumeStats := GetClientApi(a.Cli).ExamineConsumeStats(group)
	conn, err := GetClientApi(a.Cli).ExamineConsumerConnectionInfo(context.Background(), group)
	if err != nil {
		return nil, err
	}
	groupConsumeInfo.Group = group
	if consumeStats != nil {
		groupConsumeInfo.ConsumeTps = int(consumeStats.ConsumeTps)
		groupConsumeInfo.DiffTotal = consumeStats.ComputeTotalDiff()
	}
	if conn != nil {
		groupConsumeInfo.Count = len(conn.ConnectionSet)
		groupConsumeInfo.MessageModel = conn.MessageModel
		groupConsumeInfo.ConsumeType = conn.ConsumeType
		groupConsumeInfo.Version = ""
	}
	return groupConsumeInfo, nil
}

func (a *MqAdmin) DeleteSubGroup(request *DeleteSubGroupRequest) bool {
	cluster, err := a.getBrokerClusterInfo(context.Background())
	if err != nil {
		return false
	}
	if cluster == nil || len(cluster.BrokerAddrTable) == 0 {
		return false
	}
	for _, brokerName := range request.BrokerNameList {
		GetClientApi(a.Cli).DeleteSubscriptionGroup(cluster.BrokerAddrTable[brokerName].SelectBrokerAddr(), request.GroupName)
	}
	return true
}

func (a *MqAdmin) ExamineSubscriptionGroupConfig(group string) ([]*ConsumerConfigInfo, error) {
	consumerConfigInfoList := []*ConsumerConfigInfo{}
	cluster, err := a.getBrokerClusterInfo(context.Background())
	if err != nil {
		return nil, err
	}
	for key, brokerData := range cluster.BrokerAddrTable {
		addr := brokerData.SelectBrokerAddr()
		wrapper, err := a.GetAllSubscriptionGroup(context.Background(), addr, 3*time.Second)
		if err != nil {
			continue
		}
		subGroupConfig := wrapper.SubscriptionGroupTable[group]
		if &subGroupConfig == nil {
			continue
		}
		consumerConfigInfoList = append(consumerConfigInfoList, &ConsumerConfigInfo{
			BrokerNameList:          []string{key},
			SubscriptionGroupConfig: subGroupConfig,
		})
	}
	return consumerConfigInfoList, nil
}

func (a *MqAdmin) FetchBrokerNameSetBySubscriptionGroup(group string) ([]string, error) {
	brokerNameSet := []string{}
	consumerConfigs, err := a.ExamineSubscriptionGroupConfig(group)
	if err != nil {
		return nil, err
	}
	for _, config := range consumerConfigs {
		brokerNameSet = append(brokerNameSet, config.BrokerNameList...)
	}
	return brokerNameSet, nil
}

func (a *MqAdmin) ConsumerCreateOrUpdateRequest(config *ConsumerConfigInfo) (bool, error) {
	if config == nil || len(config.BrokerNameList) == 0 || len(config.ClusterNameList) == 0 {
		return false, errors.New("数据错误！")
	}
	cluster, err := a.getBrokerClusterInfo(context.Background())
	if err != nil {
		return false, err
	}
	brokerNames := config.BrokerNameList
	for _, clusterName := range config.ClusterNameList {
		brokerNames = append(brokerNames, cluster.ClusterAddrTable[clusterName]...)
	}
	for _, brokerName := range brokerNames {
		addr := cluster.BrokerAddrTable[brokerName].SelectBrokerAddr()
		err := GetClientApi(a.Cli).CreateSubscriptionGroup(addr, &config.SubscriptionGroupConfig)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}
func (a *MqAdmin) QueryConsumeStatsListByGroup(group string) ([]*TopicConsumerInfo, error) {
	return a.QueryConsumeStatsList("", group)
}

func (a *MqAdmin) QueryConsumeStatsList(topic, group string) ([]*TopicConsumerInfo, error) {
	consumeStats := GetClientApi(a.Cli).ExamineConsumeStats(group)
	mqList := []*primitive.MessageQueue{}
	for queue, _ := range consumeStats.OffsetTable {
		if topic == "" || queue.Topic == topic {
			mqList = append(mqList, queue)
		}
	}
	msgQueCliMap := a.getClientConnection(group)
	topicConsumerList := []*TopicConsumerInfo{}
	var topicConsumer *TopicConsumerInfo
	for _, queue := range mqList {
		if topicConsumer == nil || (queue.Topic != topicConsumer.Topic) {
			topicConsumer = &TopicConsumerInfo{
				Topic: queue.Topic,
			}
			topicConsumerList = append(topicConsumerList, topicConsumer)
		}
		statInfo := new(QueueStatInfo)
		statInfo.FromOffsetTableEntry(queue, consumeStats.OffsetTable[queue])
		statInfo.ClientInfo = msgQueCliMap[queue]
		topicConsumer.AppendQueueStatInfo(statInfo)
	}
	return topicConsumerList, nil
}

func (a *MqAdmin) getClientConnection(groupName string) map[*primitive.MessageQueue]string {
	conn, err := GetClientApi(a.Cli).ExamineConsumerConnectionInfo(context.Background(), groupName)
	if err != nil {
		rlog.Error("Get consumer connect for group error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}
	data := make(map[*primitive.MessageQueue]string)
	for _, connection := range conn.ConnectionSet {
		runInfo, err := a.QueryConsumerRunningInfo(groupName, connection.ClientId, false)
		if err != nil {

		}
		for queue, _ := range runInfo.MQTable {
			data[&queue] = connection.ClientId
		}
	}
	return data
}

func (a *MqAdmin) QueryConsumerRunningInfo(group, clientId string, jstack bool) (*AdminConsumerRunningInfo, error) {
	routeInfo, err := GetClientApi(a.Cli).QueryTopicRouteInfo(utils.MixAllUtil.GetRetryTopic(group))
	if err != nil {
		return nil, err
	}
	if routeInfo == nil || len(routeInfo.BrokerDataList) == 0 {
		return nil, errors.New("路由数据为空")
	}
	for _, brokerData := range routeInfo.BrokerDataList {
		addr := brokerData.SelectBrokerAddr()
		if addr != "" {
			return a.getConsumerRunningInfo(addr, group, clientId, jstack)
		}
	}

	return nil, errors.New("数据查询失败")

}
func (a *MqAdmin) getConsumerRunningInfo(addr, group, clientId string, jstack bool) (*AdminConsumerRunningInfo, error) {
	header := &internal.GetConsumerRunningInfoRequestHeader{
		ConsumerGroup: group,
		ClientId:      clientId,
		JstackEnable:  jstack,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetConsumerRunningInfo, header, nil)
	response, err := a.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch all consumerRunningInfo error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all consumerRunningInfo success", map[string]interface{}{})
	}
	if response.Code != 0 {
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	var runningInfo AdminConsumerRunningInfo
	_, err = runningInfo.Decode(response.Body, &runningInfo)
	if err != nil {
		rlog.Error("Fetch all consumerRunningInfo decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &runningInfo, nil
}

func (a *MqAdmin) FetchAllTopicList(ctx context.Context) (*TopicList, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllTopicListFromNameServer, nil, nil)
	response, err := a.Cli.InvokeSync(ctx, a.Cli.GetNameSrv().AddrList()[0], cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch all topic list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all topic list success", map[string]interface{}{})
	}
	var topicList TopicList
	_, err = topicList.Decode(response.Body, &topicList)
	if err != nil {
		rlog.Error("Fetch all topic list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &topicList, nil
}

func (a *MqAdmin) GetConsumerConnectionInfo(group string) (*ConsumerConnection, error) {
	return GetClientApi(a.Cli).ExamineConsumerConnectionInfo(context.Background(), group)
}

func (a *MqAdmin) ResetOffset(request *ResetOffsetRequest) (map[string]*ConsumerGroupRollBackStat, error) {
	groupRollbackStats := make(map[string]*ConsumerGroupRollBackStat)
	for _, group := range request.ConsumerGroupList {
		rollbackStatsMap, err := a.ResetOffsetByTimestamp(request.Topic, group, request.ResetTime, request.Force)
		groupRollbackStat := &ConsumerGroupRollBackStat{Status: true}
		if err != nil {
			if primitive.IsMQClientErr(err) {
				if (err.(*primitive.MQClientErr)).Code == 206 {
					rollbackStats := a.ResetOffsetByTimestampOld(group, request.Topic, request.ResetTime, true)
					groupRollbackStat.RollbackStatsList = rollbackStats
					groupRollbackStats[group] = groupRollbackStat
					continue
				}

			}
			groupRollbackStat.Status = false
			groupRollbackStat.ErrMsg = err.Error()
			groupRollbackStats[group] = groupRollbackStat
			continue
		}
		rollbackStatList := []*RollbackStats{}
		for queue, val := range rollbackStatsMap {
			rollbackStat := &RollbackStats{
				RollbackOffset: val,
				QueueId:        queue.QueueId,
				BrokerName:     queue.BrokerName,
			}
			rollbackStatList = append(rollbackStatList, rollbackStat)
		}
		groupRollbackStat.RollbackStatsList = rollbackStatList
		groupRollbackStats[group] = groupRollbackStat
	}
	return groupRollbackStats, nil
}

func (a *MqAdmin) ResetOffsetByTimestamp(topic, group string, timestamp int64, isForce bool) (map[primitive.MessageQueue]int64, error) {
	return a.ResetOffsetByTimestamp2(topic, group, timestamp, isForce, false)
}

func (a *MqAdmin) ResetOffsetByTimestamp2(topic, group string, timestamp int64, isForce, isC bool) (map[primitive.MessageQueue]int64, error) {
	routeInfo, _ := GetClientApi(a.Cli).QueryTopicRouteInfo(utils.MixAllUtil.GetRetryTopic(group))
	allOffsets := make(map[primitive.MessageQueue]int64)
	if routeInfo == nil || len(routeInfo.BrokerDataList) == 0 {
		return allOffsets, nil
	}
	for _, brokerData := range routeInfo.BrokerDataList {
		addr := brokerData.SelectBrokerAddr()
		if addr == "" {
			continue
		}
		offsetTabs, err := GetClientApi(a.Cli).InvokeBrokerToResetOffset2(addr, topic, group, timestamp, isForce, isC)
		if err != nil {
			return nil, err
		}
		if offsetTabs == nil {
			continue
		}
		for key, value := range offsetTabs {
			allOffsets[key] = value
		}
	}
	return allOffsets, nil
}

func (a *MqAdmin) ResetOffsetByTimestampOld(topic, group string, timestamp int64, isForce bool) []*RollbackStats {
	routeInfo, err := GetClientApi(a.Cli).QueryTopicRouteInfo(topic)
	rollbackStats := []*RollbackStats{}
	if err != nil {
		return nil
	}
	topicRouteMap := make(map[string]int)
	for _, brokerData := range routeInfo.BrokerDataList {

		for _, queue := range routeInfo.QueueDataList {
			topicRouteMap[brokerData.SelectBrokerAddr()] = queue.ReadQueueNums
		}
	}
	for _, brokerData := range routeInfo.BrokerDataList {
		addr := brokerData.SelectBrokerAddr()
		if addr == "" {
			continue
		}
		consumeStats, _ := GetClientApi(a.Cli).GetConsumeStatsNoTopic(addr, group)
		hasConsume := false
		for queue, wrapper := range consumeStats.OffsetTable {
			if topic == queue.Topic {
				hasConsume = true
				rollbackStat := a.resetOffsetConsumeOffset(addr, group, queue, wrapper, timestamp, isForce)
				rollbackStats = append(rollbackStats, rollbackStat)
			}
		}
		if !hasConsume {
			topicStat, _ := GetClientApi(a.Cli).GetTopicStatsInfo(addr, topic)
			if topicStat == nil {
				continue
			}
			for idx := 0; idx < topicRouteMap[addr]; idx++ {
				que := &primitive.MessageQueue{
					Topic:      topic,
					BrokerName: brokerData.BrokerName,
					QueueId:    idx,
				}
				wrap := &OffsetWrapper{
					BrokerOffset:   topicStat.OffsetTable[que].MaxOffset,
					ConsumerOffset: topicStat.OffsetTable[que].MinOffset,
				}
				rollbackStat := a.resetOffsetConsumeOffset(addr, group, que, wrap, timestamp, isForce)
				rollbackStats = append(rollbackStats, rollbackStat)
			}
		}
	}
	return rollbackStats
}

func (a *MqAdmin) resetOffsetConsumeOffset(brokerAddr, group string, queue *primitive.MessageQueue, wrapper *OffsetWrapper, timestamp int64, force bool) *RollbackStats {
	resetOffset := wrapper.ConsumerOffset
	if timestamp == -1 {
		resetOffset, _ = GetClientApi(a.Cli).GetMaxOffset(brokerAddr, queue.Topic, queue.QueueId)
	} else {
		resetOffset, _ = GetClientApi(a.Cli).SearchOffset(brokerAddr, queue.Topic, queue.QueueId, timestamp)
	}
	rollbackStat := &RollbackStats{
		BrokerName:      queue.BrokerName,
		QueueId:         queue.QueueId,
		BrokerOffset:    wrapper.BrokerOffset,
		ConsumerOffset:  wrapper.ConsumerOffset,
		TimestampOffset: resetOffset,
		RollbackOffset:  wrapper.ConsumerOffset,
	}
	if resetOffset < 0 {
		return rollbackStat
	}
	if force || resetOffset <= wrapper.ConsumerOffset {
		rollbackStat.RollbackOffset = resetOffset
		header := &internal.UpdateConsumerOffsetRequestHeader{
			ConsumerGroup: group,
			Topic:         queue.Topic,
			QueueId:       queue.QueueId,
			CommitOffset:  resetOffset,
		}
		err := GetClientApi(a.Cli).UpdateConsumerOffset(brokerAddr, header)
		if err != nil {
			return nil
		}
	}
	return rollbackStat
}

// CreateTopic create topic.
// TODO: another implementation like sarama, without brokerAddr as input
func (a *MqAdmin) CreateTopic(ctx context.Context, opts ...OptionCreate) error {
	cfg := defaultTopicConfigCreate()
	for _, apply := range opts {
		apply(&cfg)
	}

	request := &internal.CreateTopicRequestHeader{
		Topic:           cfg.Topic,
		DefaultTopic:    cfg.DefaultTopic,
		ReadQueueNums:   cfg.ReadQueueNums,
		WriteQueueNums:  cfg.WriteQueueNums,
		Perm:            cfg.Perm,
		TopicFilterType: cfg.TopicFilterType,
		TopicSysFlag:    cfg.TopicSysFlag,
		Order:           cfg.Order,
	}

	cmd := remote.NewRemotingCommand(internal.ReqCreateTopic, request, nil)
	_, err := a.Cli.InvokeSync(ctx, cfg.BrokerAddr, cmd, 5*time.Second)
	if err != nil {
		rlog.Error("create topic error", map[string]interface{}{
			rlog.LogKeyTopic:         cfg.Topic,
			rlog.LogKeyBroker:        cfg.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
	} else {
		rlog.Info("create topic success", map[string]interface{}{
			rlog.LogKeyTopic:  cfg.Topic,
			rlog.LogKeyBroker: cfg.BrokerAddr,
		})
	}
	return err
}

// DeleteTopicInBroker delete topic in broker.
func (a *MqAdmin) deleteTopicInBroker(ctx context.Context, topic string, brokerAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInBroker, request, nil)
	return a.Cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
}

// DeleteTopicInNameServer delete topic in nameserver.
func (a *MqAdmin) deleteTopicInNameServer(ctx context.Context, topic string, nameSrvAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInNameSrv, request, nil)
	return a.Cli.InvokeSync(ctx, nameSrvAddr, cmd, 5*time.Second)
}

// DeleteTopic delete topic in both broker and nameserver.
func (a *MqAdmin) DeleteTopic(ctx context.Context, opts ...OptionDelete) error {
	cfg := defaultTopicConfigDelete()
	for _, apply := range opts {
		apply(&cfg)
	}
	//delete topic in broker
	if cfg.BrokerAddr == "" {
		a.Cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		cfg.BrokerAddr = a.Cli.GetNameSrv().FindBrokerAddrByTopic(cfg.Topic)
	}

	if _, err := a.deleteTopicInBroker(ctx, cfg.Topic, cfg.BrokerAddr); err != nil {
		rlog.Error("delete topic in broker error", map[string]interface{}{
			rlog.LogKeyTopic:         cfg.Topic,
			rlog.LogKeyBroker:        cfg.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
		return err
	}

	//delete topic in nameserver
	if len(cfg.NameSrvAddr) == 0 {
		a.Cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		cfg.NameSrvAddr = a.Cli.GetNameSrv().AddrList()
		_, _, err := a.Cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		if err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: err,
			})
		}
		cfg.NameSrvAddr = a.Cli.GetNameSrv().AddrList()
	}

	for _, nameSrvAddr := range cfg.NameSrvAddr {
		if _, err := a.deleteTopicInNameServer(ctx, cfg.Topic, nameSrvAddr); err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				"nameServer":             nameSrvAddr,
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: err,
			})
			return err
		}
	}
	rlog.Info("delete topic success", map[string]interface{}{
		"nameServer":      cfg.NameSrvAddr,
		rlog.LogKeyTopic:  cfg.Topic,
		rlog.LogKeyBroker: cfg.BrokerAddr,
	})
	return nil
}

func (a *MqAdmin) FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error) {
	return a.Cli.GetNameSrv().FetchPublishMessageQueues(utils.WrapNamespace(a.opts.Namespace, topic))
}

func (a *MqAdmin) getBrokerClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, nil, nil)
	res, err := a.Cli.InvokeSync(ctx, a.Cli.GetNameSrv().AddrList()[0], cmd, 5*time.Second)
	if err != nil {
		rlog.Error("Fetch all clusterinfo list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}
	var cluster ClusterInfo
	_, err = cluster.Decode(res.Body, &cluster)
	if err != nil {
		rlog.Error("Fetch all clusterinfo list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &cluster, nil
}

func (a *MqAdmin) Close() error {
	a.closeOnce.Do(func() {
		a.Cli.Shutdown()
	})
	return nil
}
