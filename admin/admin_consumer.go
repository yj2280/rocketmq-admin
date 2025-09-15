package admin

import (
	"errors"
	"github.com/yj2280/rocketmq-admin/internal"
	"github.com/yj2280/rocketmq-admin/internal/utils"
	"github.com/yj2280/rocketmq-admin/primitive"
	"github.com/yj2280/rocketmq-admin/rlog"
)

/**
 * ***消费者相关操作接口
 */
func (a *MqAdmin) GetConsumerConnectionInfo(group string) (*ConsumerConnectionInfo, error) {
	consumerConn, err := GetClientApi(a.Cli).ExamineConsumerConnectionInfo(group)
	if err != nil {
		return nil, err
	}
	connSet := []*ConnectionInfo{}
	for _, connection := range consumerConn.ConnectionSet {
		connInfo := &ConnectionInfo{
			VersionDesc: GetVersionDesc(connection.Version),
			Connection:  connection,
		}
		connSet = append(connSet, connInfo)
	}
	data := &ConsumerConnectionInfo{
		ConnectionSet:      connSet,
		ConsumerConnection: consumerConn,
	}
	return data, nil
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

func (a *MqAdmin) ConsumerCreateOrUpdateRequest(config *ConsumerConfigInfo) (bool, error) {
	if config == nil || len(config.BrokerNameList) == 0 || len(config.ClusterNameList) == 0 {
		return false, errors.New("数据错误！")
	}
	cluster, err := GetClientApi(a.Cli).GetBrokerClusterInfo()
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
	OptSortUtil := InitSortCli()
	for queue, _ := range consumeStats.OffsetTable {
		if topic == "" || queue.Topic == topic {
			OptSortUtil.addData(queue)
		}
	}
	for _, comparable := range OptSortUtil.GetData() {
		mqList = append(mqList, comparable.(*primitive.MessageQueue))
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
		statInfo.ClientInfo = msgQueCliMap[*queue]

		topicConsumer.AppendQueueStatInfo(statInfo)
	}
	return topicConsumerList, nil
}

func (a *MqAdmin) QueryConsumeStatsListByTopicName(topic string) (map[string]*TopicConsumerInfo, error) {
	data := make(map[string]*TopicConsumerInfo)
	groupList, err := a.QueryTopicConsumeByWho(topic)
	if err != nil {
		return nil, err
	}
	for _, group := range groupList.GroupList {
		topicConsumers, err := a.QueryConsumeStatsList(topic, group)
		if err != nil {
			continue
		}
		topicConsumerInfo := &TopicConsumerInfo{
			Topic: topic,
		}
		if len(topicConsumers) > 0 {
			topicConsumerInfo = topicConsumers[0]
		}
		data[group] = topicConsumerInfo
	}
	return data, nil
}

func (a *MqAdmin) QueryTopicConsumeByWho(topic string) (*GroupList, error) {
	routeInfo, err := GetClientApi(a.Cli).QueryTopicRouteInfo(topic)
	if err != nil {
		return nil, err
	}
	if routeInfo == nil || len(routeInfo.BrokerDataList) == 0 {
		return nil, errors.New("路由数据为空")
	}
	for _, brokerData := range routeInfo.BrokerDataList {
		addr := brokerData.SelectBrokerAddr()
		if addr != "" {
			return GetClientApi(a.Cli).QueryTopicConsumeByWho(addr, topic)
		}
	}
	return nil, nil
}

func (a *MqAdmin) getClientConnection(groupName string) map[primitive.MessageQueue]string {
	conn, err := GetClientApi(a.Cli).ExamineConsumerConnectionInfo(groupName)
	if err != nil {
		rlog.Error("Get consumer connect for group error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}
	data := make(map[primitive.MessageQueue]string)
	if conn == nil {
		return data
	}
	for _, connection := range conn.ConnectionSet {
		runInfo, err := a.QueryConsumerRunningInfo(groupName, connection.ClientId, false)
		if err != nil || runInfo == nil {
			continue
		}
		for queue, _ := range runInfo.MQTable {
			data[queue] = connection.ClientId
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
			return GetClientApi(a.Cli).GetConsumerRunningInfo(addr, group, clientId, jstack)
		}
	}

	return nil, errors.New("数据查询失败")

}
