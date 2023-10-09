package admin

import (
	"context"
	"errors"
	"github.com/slh92/rocketmq-admin/consumer"
	"github.com/slh92/rocketmq-admin/internal"
	"github.com/slh92/rocketmq-admin/internal/remote"
	"github.com/slh92/rocketmq-admin/internal/utils"
	"github.com/slh92/rocketmq-admin/primitive"
	"github.com/slh92/rocketmq-admin/rlog"
	"sort"
	"strings"
	"sync"
	"time"
)

type TrackType string

const (
	CONSUMED              = TrackType("CONSUMED")
	CONSUMED_BUT_FILTERED = TrackType("CONSUMED_BUT_FILTERED")
	PULL                  = TrackType("PULL")
	NOT_CONSUME_YET       = TrackType("NOT_CONSUME_YET")
	NOT_ONLINE            = TrackType("NOT_ONLINE")
	UNKNOWN               = TrackType("UNKNOWN")
)

type MqConsumerApi struct {
	Cli internal.RMQClient
}

func GetConsumerApi(cli internal.RMQClient) *MqConsumerApi {
	consumerApi := &MqConsumerApi{
		Cli: cli,
	}
	return consumerApi
}

func (c *MqConsumerApi) QueryMessageByTopic(topic string, begin, end int64) ([]*MessageView, error) {
	nameServ, _ := primitive.NewNamesrvAddr(c.Cli.GetNameSrv().AddrList()[0])
	pc, _ := consumer.NewPullConsumer(
		consumer.WithGroupName(utils.TOOLS_CONSUMER_GROUP),
		consumer.WithNameServer(nameServ),
		consumer.WithMaxReconsumeTimes(2),
	)
	msgList := []*MessageView{}
	err := pc.Start()
	if err != nil {
		return nil, err
	}
	queues, err := c.Cli.GetNameSrv().FetchSubscribeMessageQueues(topic)
	if err != nil {
		return nil, err
	}
	OptSortUtil := InitSortCli()
	for _, queue := range queues {
		minOffset, _ := c.searchOffset(queue, begin)
		maxOffset, _ := c.searchOffset(queue, end)
	READQ:
		for off := minOffset; off <= maxOffset; {
			if len(msgList) > 2000 {
				break
			}
			pullres, err := pc.PullFrom(context.Background(), queue, off, 32)
			if err != nil {
				continue
			}
			off = pullres.NextBeginOffset
			switch pullres.Status {
			case primitive.PullFound:
				for _, ext := range pullres.GetMessageExts() {
					ext.Body = nil
					if ext.StoreTimestamp < begin || ext.StoreTimestamp > end {
						continue
					}
					view := MessageView(*ext)
					OptSortUtil.addData(&view)
				}

				break
			case primitive.PullNoNewMsg:
			case primitive.PullNoMsgMatched:
			case primitive.PullOffsetIllegal:
				break READQ
			}

		}
	}

	sort.Sort(OptSortUtil)
	for _, comparable := range OptSortUtil.GetData() {
		msgList = append(msgList, comparable.(*MessageView))
	}
	return msgList, nil
}

func (c *MqConsumerApi) QueryMessageByTopicAndKey(topic, key string) ([]*MessageView, error) {
	res, err := c.queryMessage(topic, key, 32, 0, time.Now().UnixMilli(), false)
	if err != nil {
		return nil, err
	}
	return res.MessageList, nil
}

func (c *MqConsumerApi) ViewMessage(topic, msgId string) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	view, tracks, err := c.viewMessage(topic, msgId)
	if err != nil {
		return nil, err
	}
	data["messageView"] = view
	data["messageTrackList"] = tracks
	return data, nil

}

func (c *MqConsumerApi) viewMessage(topic, msgId string) (*MessageView, []*MessageTrack, error) {
	res, err := c.queryMessage(topic, msgId, 32, 0, time.Now().UnixMilli(), true)
	if err != nil {
		return nil, nil, err
	}
	var view *MessageView
	var tracks []*MessageTrack
	if res != nil && len(res.MessageList) > 0 {
		view = res.MessageList[0]
	}

	if view != nil {
		tracks, err = c.messageTrackDetail(view)
		if err != nil {
			return nil, nil, err
		}
	}
	return view, tracks, nil
}

func (c *MqConsumerApi) messageTrackDetail(view *MessageView) ([]*MessageTrack, error) {
	routeInfo, err := GetClientApi(c.Cli).QueryTopicRouteInfo(view.Topic)
	if err != nil {
		return nil, err
	}
	if routeInfo == nil || len(routeInfo.BrokerDataList) == 0 {
		return nil, errors.New("路由数据为空")
	}
	var groupList *GroupList
	for _, brokerData := range routeInfo.BrokerDataList {
		addr := brokerData.SelectBrokerAddr()
		if addr != "" {
			groupList, _ = GetClientApi(c.Cli).QueryTopicConsumeByWho(addr, view.Topic)
			break
		}
	}
	if groupList == nil {
		return nil, errors.New("消费分组查询失败")
	}
	result := []*MessageTrack{}
	for _, group := range groupList.GroupList {
		mt := &MessageTrack{
			ConsumerGroup: group,
			TrackType:     string(UNKNOWN),
		}
		cc, err := GetClientApi(c.Cli).ExamineConsumerConnectionInfo(group)
		if err != nil {
			if primitive.IsMQBrokerErr(err) {
				if err.(primitive.MQBrokerErr).ResponseCode == 206 {
					mt.TrackType = string(NOT_ONLINE)
				}
			}
			mt.ExceptionDesc = err.Error()
			result = append(result, mt)
			continue
		}
		switch cc.ConsumeType {
		case CONSUME_ACTIVELY:
			mt.TrackType = string(PULL)
			break
		case CONSUME_PASSIVELY:
			ifConsumed, err := c.consumed(view, group)
			if err != nil {
				if primitive.IsMQClientErr(err) {
					if err.(primitive.MQClientErr).Code == 206 {
						mt.TrackType = string(NOT_ONLINE)
					}
				} else if primitive.IsMQBrokerErr(err) {
					if err.(primitive.MQBrokerErr).ResponseCode == 206 {
						mt.TrackType = string(NOT_ONLINE)
					}
				}
				mt.ExceptionDesc = err.Error()
				result = append(result, mt)
			}
			if ifConsumed {
				mt.TrackType = string(CONSUMED)
				for _, subscriptionData := range cc.SubscriptionTable {
					tags := strings.Join(subscriptionData.TagsSet, ",")
					if tags == "" || strings.Contains(tags, view.GetTags()) || strings.Contains(tags, "*") {
						continue
					}
					mt.TrackType = string(CONSUMED_BUT_FILTERED)
				}
			} else {
				mt.TrackType = string(NOT_CONSUME_YET)
			}
			break
		default:
			break
		}
		result = append(result, mt)
	}
	return result, nil
}
func (c *MqConsumerApi) consumed(msg *MessageView, group string) (bool, error) {
	consumeStat := GetClientApi(c.Cli).ExamineConsumeStats(group)
	if consumeStat == nil {
		return false, errors.New("consumeStat数据查询失败")
	}
	cluster, err := GetClientApi(c.Cli).GetBrokerClusterInfo()
	if err != nil {
		return false, err
	}
	if cluster == nil {
		return false, errors.New("cluster数据查询失败")
	}
	if len(consumeStat.OffsetTable) > 0 {
		for queue, wrapper := range consumeStat.OffsetTable {
			if queue.Topic != msg.Topic || queue.QueueId != msg.Queue.QueueId {
				continue
			}
			broker := cluster.BrokerAddrTable[queue.BrokerName]
			if broker == nil {
				continue
			}
			if broker.SelectBrokerAddr() != msg.StoreHost {
				continue
			}
			if wrapper.ConsumerOffset > msg.QueueOffset {
				return true, nil
			}

		}
	}
	return false, errors.New("数据查询失败")
}
func (c *MqConsumerApi) queryMessage(topic, key string, maxNum int, begin, end int64, isUniqKey bool) (*QueryResult, error) {
	route, err := GetClientApi(c.Cli).QueryTopicRouteInfo(topic)
	if err != nil {
		return nil, err
	}
	if route == nil {
		c.Cli.GetNameSrv().UpdateTopicRouteInfo(topic)
		route, err = c.Cli.GetNameSrv().FindRouteInfoByTopic(topic)
		if err != nil {
			return nil, err
		}
	}
	if route != nil {
		brokerAddrs := []string{}
		for _, brokerData := range route.BrokerDataList {
			if brokerData.SelectBrokerAddr() != "" {
				brokerAddrs = append(brokerAddrs, brokerData.SelectBrokerAddr())
			}
		}
		if len(brokerAddrs) > 0 {
			wg := sync.WaitGroup{}
			mu := sync.Mutex{}
			wg.Add(len(brokerAddrs))
			queryResultList := []*QueryResult{}
			header := &internal.QueryMessageRequestHeader{
				Topic:          topic,
				Key:            key,
				MaxNum:         maxNum,
				BeginTimestamp: begin,
				EndTimestamp:   end,
			}
			for _, addr := range brokerAddrs {
				GetClientApi(c.Cli).queryMessage(addr, header, func(command *remote.RemotingCommand, err error) {
					if command != nil {
						switch command.Code {
						case 0:
							respHeader := new(QueryMessageResponseHeader)
							respHeader.Decode(command.ExtFields)
							msgExt := []*MessageView{}
							remoteSai := &RemotingSerializable{}
							_, err := remoteSai.Decode(command.Body, &msgExt)
							if err != nil {
								rlog.Error("Fetch all query broker message decode error", map[string]interface{}{
									rlog.LogKeyMessages: err,
								})
								return
							}
							qr := &QueryResult{
								IndexLastUpdateTimestamp: respHeader.IndexLastUpdateTimestamp,
								MessageList:              msgExt,
							}
							mu.Lock()
							queryResultList = append(queryResultList, qr)
							defer mu.Unlock()
							return
						default:
							rlog.Error("Fetch all query broker message decode error", map[string]interface{}{
								rlog.LogKeyMessages: command.Remark,
							})
						}
					}
					wg.Done()
				}, isUniqKey)
			}
			wg.Wait()
			indexLastUpdateTimestamp := int64(0)
			msgList := []*MessageView{}
			for _, queryResult := range queryResultList {
				if queryResult.IndexLastUpdateTimestamp > indexLastUpdateTimestamp {
					indexLastUpdateTimestamp = queryResult.IndexLastUpdateTimestamp
				}
				for _, view := range queryResult.MessageList {
					if isUniqKey {
						if view.MsgId != key {
							continue
						}
						if len(msgList) == 0 {
							continue
						}
						if msgList[0].StoreTimestamp > view.StoreTimestamp {
							msgList = []*MessageView{}
						}
						msgList = append(msgList, view)
						rlog.Error("queryMessage by uniqKey, find message key not matched, maybe hash duplicate", map[string]interface{}{
							rlog.LogKeyMessages: view.String(),
						})

					} else {
						if view.GetKeys() == "" {
							continue
						}
						keyList := strings.Split(view.GetKeys(), " ")
						for _, keystr := range keyList {
							if keystr == key {
								msgList = append(msgList, view)
								break
							}
						}

					}
				}
			}
			if len(msgList) > 0 {
				return &QueryResult{
					IndexLastUpdateTimestamp: indexLastUpdateTimestamp,
					MessageList:              msgList,
				}, nil
			}

		}
	}
	return nil, errors.New("数据查询失败")

}

func (c *MqConsumerApi) searchOffset(queue *primitive.MessageQueue, timestamp int64) (int64, error) {
	brokerAddr := c.Cli.GetNameSrv().FindBrokerAddrByName(queue.BrokerName)
	if brokerAddr == "" {
		c.Cli.GetNameSrv().UpdateTopicRouteInfo(queue.Topic)
		brokerAddr = c.Cli.GetNameSrv().FindBrokerAddrByName(queue.BrokerName)
	}
	if brokerAddr != "" {
		return GetClientApi(c.Cli).SearchOffset(brokerAddr, queue.Topic, queue.QueueId, timestamp)
	}
	return 0, nil
}
