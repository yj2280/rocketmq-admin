package admin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/yj2280/rocketmq-admin/internal"
	"github.com/yj2280/rocketmq-admin/internal/remote"
	"github.com/yj2280/rocketmq-admin/internal/utils"
	"github.com/yj2280/rocketmq-admin/primitive"
	"github.com/yj2280/rocketmq-admin/rlog"
)

type MqClientApi struct {
	Cli internal.RMQClient
}

func GetClientApi(cli internal.RMQClient) *MqClientApi {
	clientApi := &MqClientApi{
		Cli: cli,
	}
	return clientApi
}

func (c *MqClientApi) InvokeBrokerToResetOffset(addr, topic, group string, timestamp int64, isForce bool) (map[primitive.MessageQueue]int64, error) {
	return c.InvokeBrokerToResetOffset2(addr, topic, group, timestamp, isForce, false)
}

func (c *MqClientApi) InvokeBrokerToResetOffset2(addr, topic, group string, timestamp int64, isForce, isC bool) (map[primitive.MessageQueue]int64, error) {
	header := &internal.ResetOffsetHeader{
		Topic:     topic,
		Group:     group,
		Timestamp: timestamp,
		IsForce:   isForce,
	}
	cmd := remote.NewRemotingCommand(internal.ReqInvokeBrokerToResetOffset, header, nil)
	if isC {
		cmd.Language = remote.CPP
	}
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("exec resetOffset error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("exec resetOffset success", map[string]interface{}{})
	}
	if response.Code != 0 {
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
	}

	offsetBody := new(internal.ResetOffsetBody)
	offsetBody.Decode(response.Body)

	return offsetBody.OffsetTable, nil
}

func (c *MqClientApi) ExamineConsumeStats(group string) *ConsumeStats {
	return c.ExamineConsumeStatsWithTopic(group, "")
}

func (c *MqClientApi) ExamineConsumeStatsWithTopic(group string, topic string) *ConsumeStats {
	routeInfo, err := c.QueryTopicRouteInfo(utils.MixAllUtil.GetRetryTopic(group))
	result := &ConsumeStats{}
	if err != nil {
		return result
	}
	type Alsa ConsumeStats
	for _, bd := range routeInfo.BrokerDataList {
		addr := bd.SelectBrokerAddr()
		if addr != "" {
			header := &internal.GetConsumeStatsRequestHeader{
				ConsumerGroup: group,
				Topic:         topic,
			}
			cmd := remote.NewRemotingCommand(internal.ReqGetConsumerStatsFromServer, header, nil)
			response, err := c.Cli.InvokeSync(context.Background(), addr, cmd, 10*time.Second)
			if err != nil {
				rlog.Error("Fetch all consumerConnectiion list error", map[string]interface{}{
					rlog.LogKeyUnderlayError: err,
				})
				return nil
			} else {
				rlog.Info("Fetch all consumerConnectiion list success", map[string]interface{}{})
			}
			var stats ConsumeStats
			aux := &struct {
				OffsetTable map[string]*OffsetWrapper `json:"offsetTable"`
				*Alsa
			}{
				Alsa: (*Alsa)(&stats),
			}
			_, err = stats.Decode(response.Body, &aux)
			if err != nil {
				rlog.Error("Fetch all consumerConnectiion list decode error", map[string]interface{}{
					rlog.LogKeyUnderlayError: err,
				})
				return nil
			}
			if result.OffsetTable == nil {
				result.OffsetTable = make(map[*primitive.MessageQueue]*OffsetWrapper)
			}
			for key, val := range aux.OffsetTable {
				var queue *primitive.MessageQueue
				stats.FromJson(key, &queue)
				result.OffsetTable[queue] = val
			}
			result.ConsumeTps += aux.ConsumeTps
		}
	}
	return result
}

func (c *MqClientApi) GetConsumeStatsNoTopic(addr, group string) (*ConsumeStats, error) {
	return c.GetConsumeStats(addr, group, "")
}

func (c *MqClientApi) GetConsumeStats(addr, group, topic string) (*ConsumeStats, error) {
	header := &internal.GetConsumeStatsRequestHeader{
		ConsumerGroup: group,
		Topic:         topic,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetConsumerStatsFromServer, header, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("Fetch all consumerConnectiion list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all consumerConnectiion list success", map[string]interface{}{})
	}
	if response == nil {
		return nil, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	var stats ConsumeStats
	type Alsa ConsumeStats
	aux := &struct {
		OffsetTable map[string]*OffsetWrapper `json:"offsetTable"`
		*Alsa
	}{
		Alsa: (*Alsa)(&stats),
	}
	_, err = stats.Decode(response.Body, &aux)
	if err != nil {
		rlog.Error("Fetch all consumerConnectiion list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	if stats.OffsetTable == nil {
		stats.OffsetTable = make(map[*primitive.MessageQueue]*OffsetWrapper)
	}
	for key, val := range aux.OffsetTable {
		var queue *primitive.MessageQueue
		stats.FromJson(key, &queue)
		stats.OffsetTable[queue] = val
	}
	return &stats, nil
}

func (c *MqClientApi) QueryTopicRouteInfo(topic string) (*internal.TopicRouteData, error) {
	return c.Cli.GetNameSrv().FindRouteInfoByTopic(topic)
}

func (c *MqClientApi) QueryTopicConsumeByWho(addr, topic string) (*GroupList, error) {
	header := &internal.QueryTopicConsumeByWhoRequestHeader{
		Topic: topic,
	}
	cmd := remote.NewRemotingCommand(internal.QueryTopicConsumeByWho, header, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("Fetch all topic consumer error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all topic consumer success", map[string]interface{}{})
	}
	if response == nil {
		return nil, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	var groupList GroupList
	_, err = groupList.Decode(response.Body, &groupList)
	if err != nil {
		rlog.Error("Fetch all topic consumer list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &groupList, nil
}

func (c *MqClientApi) GetConsumerRunningInfo(addr, group, clientId string, jstack bool) (*AdminConsumerRunningInfo, error) {
	header := &internal.GetConsumerRunningInfoRequestHeader{
		ConsumerGroup: group,
		ClientId:      clientId,
		JstackEnable:  jstack,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetConsumerRunningInfo, header, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
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
	type Alsa AdminConsumerRunningInfo
	aux := &struct {
		MqTab   map[string]internal.ProcessQueueInfo `json:"mqTable"`
		SubData map[string]bool                      `json:"subscriptionData"`
		*Alsa
	}{
		Alsa: (*Alsa)(&runningInfo),
	}
	_, err = runningInfo.Decode(response.Body, &aux)
	if err != nil {
		rlog.Error("Fetch all consumerRunningInfo decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	if runningInfo.MQTable == nil {
		runningInfo.MQTable = make(map[primitive.MessageQueue]internal.ProcessQueueInfo)
	}

	if runningInfo.SubscriptionData == nil {
		runningInfo.SubscriptionData = make(map[*internal.SubscriptionData]bool)
	}
	for key, val := range aux.MqTab {
		var queue primitive.MessageQueue
		runningInfo.FromJson(key, &queue)
		runningInfo.MQTable[queue] = val
	}

	for key, val := range aux.SubData {
		var subdata *internal.SubscriptionData
		runningInfo.FromJson(key, &subdata)
		runningInfo.SubscriptionData[subdata] = val
	}
	return &runningInfo, nil
}

func (c *MqClientApi) ExamineConsumerConnectionInfo(group string) (*ConsumerConnection, error) {

	routeInfo, err := c.QueryTopicRouteInfo(utils.MixAllUtil.GetRetryTopic(group))
	if routeInfo == nil || len(routeInfo.BrokerDataList) == 0 {
		return nil, errors.New("路由数据不能为空")
	}
	if err != nil {
		return nil, err
	}
	brokerInfo := routeInfo.BrokerDataList[0]
	if brokerInfo != nil && brokerInfo.SelectBrokerAddr() != "" {
		conn, err := c.GetConsumerConnectionList(brokerInfo.SelectBrokerAddr(), group)
		if err != nil {
			return nil, err
		}
		if conn == nil || len(conn.ConnectionSet) == 0 {
			return nil, primitive.MQClientErr{
				Code: 206,
				Msg:  "Not found the consumer group connection",
			}
		}
		return conn, nil
	}
	return nil, errors.New("数据查询失败")
}

func (c *MqClientApi) GetConsumerConnectionList(addr, group string) (*ConsumerConnection, error) {
	header := &internal.GetConsumerConnectionListRequestHeader{
		ConsumerGroup: group,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetConsumerConnectionList, header, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("Fetch all consumerConnectiion list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all consumerConnectiion list success", map[string]interface{}{})
	}
	if response == nil {
		return nil, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return nil, primitive.NewMQBrokerErr(response.Code, response.Remark)
	}
	var connect ConsumerConnection
	_, err = connect.Decode(response.Body, &connect)
	if err != nil {
		rlog.Error("Fetch all consumerConnectiion list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &connect, nil
}

func (c *MqClientApi) GetMaxOffset(addr, topic string, queueId int) (int64, error) {
	header := &internal.GetMaxOffsetRequestHeader{
		Topic:   topic,
		QueueId: queueId,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetMaxOffset, header, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("Get maxoffset error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return -1, err
	} else {
		rlog.Info("Get maxoffset success", map[string]interface{}{})
	}
	if response == nil {
		return -1, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return -1, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	return strconv.ParseInt(response.ExtFields["offset"], 10, 64)
}

func (c *MqClientApi) SearchOffset(addr, topic string, queueId int, timestamp int64) (int64, error) {
	header := &internal.SearchOffsetRequestHeader{
		Topic:     topic,
		QueueId:   queueId,
		Timestamp: timestamp,
	}
	cmd := remote.NewRemotingCommand(internal.ReqSearchOffsetByTimestamp, header, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("Search Offset error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return -1, err
	} else {
		rlog.Info("Search Offset success", map[string]interface{}{})
	}
	if response == nil {
		return -1, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return -1, primitive.NewMQBrokerErr(response.Code, response.Remark)
	}
	return strconv.ParseInt(response.ExtFields["offset"], 10, 64)
}

func (c *MqClientApi) GetTopicStatsInfo(addr, topic string) (*TopicStatsTable, error) {
	header := &internal.GetTopicStatsInfoRequestHeader{
		Topic: topic,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetTopicStats, header, nil)
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if response == nil {
		return nil, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	var stats TopicStatsTable
	type Alsa TopicStatsTable
	aux := &struct {
		OffsetTable map[string]*TopicOffset `json:"offsetTable"`
		*Alsa
	}{
		Alsa: (*Alsa)(&stats),
	}
	_, err := stats.Decode(response.Body, &aux)
	if err != nil {
		return nil, err
	}
	if stats.OffsetTable == nil {
		stats.OffsetTable = make(map[*primitive.MessageQueue]*TopicOffset)
	}
	for key, val := range aux.OffsetTable {
		var queue *primitive.MessageQueue
		stats.FromJson(key, &queue)
		stats.OffsetTable[queue] = val
	}
	return &stats, nil
}

func (c *MqClientApi) GetSystemTopicList(client *MqAdmin) (*TopicList, error) {

	for _, nameServer := range c.Cli.GetNameSrv().AddrList() {
		cmd := remote.NewRemotingCommand(internal.GET_SYSTEM_TOPIC_LIST_FROM_NS, nil, nil)
		response, err := c.Cli.InvokeSync(context.Background(), nameServer, cmd, 3*time.Second)
		fmt.Println(response, err)
	}
	brokerNameList := make([]string, 0)
	clusterInfo, err := GetClientApi(client.Cli).GetBrokerClusterInfo()
	fmt.Println("clusterInfo", clusterInfo, err)
	for _, brokerAddr := range clusterInfo.BrokerAddrTable {
		brokerNameList = append(brokerNameList, brokerAddr.BrokerAddresses[0])
	}
	for _, brokerAddr := range brokerNameList {
		cmd := remote.NewRemotingCommand(internal.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, nil, nil)
		response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(brokerAddr), cmd, 10*time.Second)
		fmt.Println(response, err)
	}
	cmd := remote.NewRemotingCommand(internal.GET_SYSTEM_TOPIC_LIST_FROM_NS, nil, nil)
	response, err := c.Cli.InvokeSync(context.Background(), c.Cli.GetNameSrv().AddrList()[0], cmd, 3*time.Second)

	if err != nil {
		rlog.Error("Fetch system topic list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch system topic list success", map[string]interface{}{})
	}
	var topicList TopicList
	_, err = topicList.Decode(response.Body, &topicList)
	if err != nil {
		rlog.Error("Fetch system topic list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &topicList, nil
}

func (c *MqClientApi) GetSystemTopicListFromBrokerList(brokerAddrList []string) (map[string]TopicList, error) {
	topicMap := make(map[string]TopicList)
	for _, brokerAddr := range brokerAddrList {
		cmd := remote.NewRemotingCommand(internal.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, nil, nil)
		response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(brokerAddr), cmd, 10*time.Second)
		var topicList TopicList
		if err != nil {
			rlog.Error("Fetch System topic list error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			return nil, err
		} else {
			rlog.Info("Fetch system topic list success", map[string]interface{}{})
		}
		_, err = topicList.Decode(response.Body, &topicList)
		topicMap[brokerAddr] = topicList
	}

	return topicMap, nil
}

func (c *MqClientApi) UpdateConsumerOffset(addr string, header *internal.UpdateConsumerOffsetRequestHeader) error {
	cmd := remote.NewRemotingCommand(internal.ReqUpdateConsumerOffset, header, nil)
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if response == nil {
		return errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return primitive.NewMQClientErr(response.Code, response.Remark)
	}
	return nil
}

func (c *MqClientApi) DeleteSubscriptionGroup(addr, group string) error {
	header := &internal.DeleteSubscriptionGroupRequestHeader{
		GroupName:    group,
		RemoveOffset: "true",
	}
	cmd := remote.NewRemotingCommand(internal.ReqDeleteGroupInBroker, header, nil)
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if response == nil {
		return errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return primitive.NewMQClientErr(response.Code, response.Remark)
	}
	return nil
}

func (c *MqClientApi) GetBrokerClusterInfo() (*ClusterInfo, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, nil, nil)
	res, err := c.Cli.InvokeSync(context.Background(), c.Cli.GetNameSrv().AddrList()[0], cmd, 10*time.Second)
	if err != nil {
		rlog.Error("Fetch all clusterinfo list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
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

func (c *MqClientApi) queryMessage(addr string, header *internal.QueryMessageRequestHeader, callbak func(*remote.RemotingCommand, error), isUnqiKey bool) {
	cmd := remote.NewRemotingCommand(internal.ReqQueryMessage, header, nil)
	cmd.ExtFields["_UNIQUE_KEY_QUERY"] = strconv.FormatBool(isUnqiKey)
	c.Cli.InvokeAsync(context.Background(), internal.BrokerVIPChannel(addr), cmd, callbak)
}

func (c *MqClientApi) viewMessage(addr string, phyoffset int64) (*MessageView, error) {
	header := &internal.ViewMessageRequestHeader{
		Offset: phyoffset,
	}
	cmd := remote.NewRemotingCommand(internal.ReqViewMessageByID, header, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("view messageById error", map[string]interface{}{
			rlog.LogKeyMessages: err,
		})
		return nil, err
	}
	if response == nil {
		return nil, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return nil, primitive.NewMQBrokerErr(response.Code, response.Remark)
	}
	var view MessageView
	msgList := primitive.DecodeMessage(response.Body)
	if len(msgList) > 0 {
		view = MessageView{
			MessageExt: msgList[0],
		}
		view.MessageBody = string(view.Body)
	}
	return &view, nil
}

func (c *MqClientApi) ConsumeMessageDirectly(addr, group, clientId, msgId string) (*ConsumeMessageDirectlyResult, error) {
	header := &internal.ConsumeMessageDirectlyHeader{
		ConsumerGroup: group,
		ClientID:      clientId,
		MsgId:         msgId,
	}
	cmd := remote.NewRemotingCommand(internal.ReqConsumeMessageDirectly, header, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 20*time.Second)
	if err != nil {
		rlog.Error("ConsumeMessageDirectly error", map[string]interface{}{
			rlog.LogKeyMessages: err,
		})
		return nil, err
	}
	if response == nil {
		return nil, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	var directRes ConsumeMessageDirectlyResult
	_, err = directRes.Decode(response.Body, &directRes)
	if err != nil {
		return nil, err
	}
	return &directRes, nil
}

func (c *MqClientApi) CreateSubscriptionGroup(addr string, config *SubscriptionGroupConfig) error {
	cmd := remote.NewRemotingCommand(internal.ReqUpdateCreateSubscriptionGroup, nil, nil)
	remoteSerize := &RemotingSerializable{}
	body, err := remoteSerize.Encode(config)
	if err != nil {
		return err
	}
	cmd.Body = body
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if response == nil {
		return errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return primitive.NewMQClientErr(response.Code, response.Remark)
	}
	return nil
}

func (c *MqClientApi) getBrokerRuntimeInfo(addr string) (*KVTable, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerRuntimeInfo, nil, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("get broker runtimeinfo error", map[string]interface{}{
			rlog.LogKeyMessages: err,
		})
		return nil, err
	}
	if response == nil {
		return nil, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return nil, primitive.NewMQBrokerErr(response.Code, response.Remark)
	}
	var kv KVTable
	_, err = kv.Decode(response.Body, &kv)
	if err != nil {
		return nil, err
	}
	return &kv, nil
}

func (c *MqClientApi) getBrokerConfig(addr string) (*map[string]string, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerConfig, nil, nil)
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 10*time.Second)
	if err != nil {
		rlog.Error("get broker config error", map[string]interface{}{
			rlog.LogKeyMessages: err,
		})
		return nil, err
	}
	if response == nil {
		return nil, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return nil, primitive.NewMQBrokerErr(response.Code, response.Remark)
	}
	kv := utils.MixAllUtil.StringToProperties(string(response.Body))
	return &kv, nil
}
