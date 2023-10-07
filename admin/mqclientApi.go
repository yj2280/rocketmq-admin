package admin

import (
	"context"
	"errors"
	"github.com/slh92/rocketmq-admin/internal"
	"github.com/slh92/rocketmq-admin/internal/remote"
	"github.com/slh92/rocketmq-admin/internal/utils"
	"github.com/slh92/rocketmq-admin/primitive"
	"github.com/slh92/rocketmq-admin/rlog"
	"time"
)

type MqClientApi struct {
	Cli internal.RMQClient
}

var clientApi = &MqClientApi{}

func GetClientApi(cli internal.RMQClient) *MqClientApi {
	clientApi.Cli = cli
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
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
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
			response, err := c.Cli.InvokeSync(context.Background(), addr, cmd, 3*time.Second)
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
	response, err := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch all consumerConnectiion list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all consumerConnectiion list success", map[string]interface{}{})
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

func (c *MqClientApi) ExamineConsumerConnectionInfo(ctx context.Context, group string) (*ConsumerConnection, error) {
	header := &internal.GetConsumerConnectionListRequestHeader{
		ConsumerGroup: group,
	}
	routeInfo, err := c.QueryTopicRouteInfo(utils.MixAllUtil.GetRetryTopic(group))
	if routeInfo == nil || len(routeInfo.BrokerDataList) == 0 {
		return nil, errors.New("路由数据不能为空")
	}
	brokerInfo := routeInfo.BrokerDataList[0]
	cmd := remote.NewRemotingCommand(internal.ReqGetConsumerConnectionList, header, nil)
	response, err := c.Cli.InvokeSync(ctx, brokerInfo.SelectBrokerAddr(), cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch all consumerConnectiion list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all consumerConnectiion list success", map[string]interface{}{})
	}
	if response.Code != 0 {
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
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
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
	if response == nil {
		return -1, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return -1, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	var maxOffset GetMaxOffsetResponseHeader
	_, err := maxOffset.Decode(response.Body, &maxOffset)
	if err != nil {
		return -1, err
	}
	return maxOffset.Offset, nil
}

func (c *MqClientApi) SearchOffset(addr, topic string, queueId int, timestamp int64) (int64, error) {
	header := &internal.SearchOffsetRequestHeader{
		Topic:     topic,
		QueueId:   queueId,
		Timestamp: timestamp,
	}
	cmd := remote.NewRemotingCommand(internal.ReqSearchOffsetByTimestamp, header, nil)
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
	if response == nil {
		return -1, errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return -1, primitive.NewMQClientErr(response.Code, response.Remark)
	}
	var maxOffset GetMaxOffsetResponseHeader
	_, err := maxOffset.Decode(response.Body, &maxOffset)
	if err != nil {
		return -1, err
	}
	return maxOffset.Offset, nil
}

func (c *MqClientApi) GetTopicStatsInfo(addr, topic string) (*TopicStatsTable, error) {
	header := &internal.GetTopicStatsInfoRequestHeader{
		Topic: topic,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetTopicStats, header, nil)
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
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

func (c *MqClientApi) UpdateConsumerOffset(addr string, header *internal.UpdateConsumerOffsetRequestHeader) error {
	cmd := remote.NewRemotingCommand(internal.ReqUpdateConsumerOffset, header, nil)
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
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
		GroupName: group,
	}
	cmd := remote.NewRemotingCommand(internal.ReqDeleteGroupInBroker, header, nil)
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
	if response == nil {
		return errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return primitive.NewMQClientErr(response.Code, response.Remark)
	}
	return nil
}

func (c *MqClientApi) CreateSubscriptionGroup(addr string, config *SubscriptionGroupConfig) error {
	cmd := remote.NewRemotingCommand(internal.ReqUpdateCreateSubscriptionGroup, nil, nil)
	remoteSerize := &RemotingSerializable{}
	body, err := remoteSerize.Encode(config)
	if err != nil {
		return err
	}
	cmd.Body = body
	response, _ := c.Cli.InvokeSync(context.Background(), internal.BrokerVIPChannel(addr), cmd, 3*time.Second)
	if response == nil {
		return errors.New("远程响应失败！")
	}
	if response.Code != 0 {
		return primitive.NewMQClientErr(response.Code, response.Remark)
	}
	return nil
}
