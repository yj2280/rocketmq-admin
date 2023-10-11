package admin

import (
	"context"
	"errors"
	"github.com/slh92/rocketmq-admin/internal/utils"
	"github.com/slh92/rocketmq-admin/primitive"
	"github.com/slh92/rocketmq-admin/producer"
	"time"
)

/**
 * ***消息相关操作接口
 */
func (a *MqAdmin) QueryMessageByTopic(topic string, begin, end int64) ([]*MessageView, error) {
	return GetConsumerApi(a.Cli).QueryMessageByTopic(topic, begin, end)
}

func (a *MqAdmin) QueryMessageByTopicAndKey(topic, key string) ([]*MessageView, error) {
	return GetConsumerApi(a.Cli).QueryMessageByTopicAndKey(topic, key)
}

func (a *MqAdmin) ViewMessage(topic, msgId string) (map[string]interface{}, error) {
	return GetConsumerApi(a.Cli).ViewMessage(topic, msgId)
}
func (a *MqAdmin) ConsumeMessageDirectly(topic, group, clientId, msgId string) (*ConsumeMessageDirectlyResult, error) {
	if clientId != "" {
		return a.consumeMessageDirectlyForMsg(group, topic, msgId, clientId)
	}
	conn, err := GetClientApi(a.Cli).ExamineConsumerConnectionInfo(group)
	if err != nil {
		return nil, err
	}
	for _, connection := range conn.ConnectionSet {
		if connection.ClientId == "" {
			continue
		}
		return a.consumeMessageDirectlyForMsg(group, topic, msgId, connection.ClientId)
	}
	return nil, errors.New("数据发送失败")
}

func (a *MqAdmin) consumeMessageDirectlyForMsg(group, topic, msgId, clientId string) (*ConsumeMessageDirectlyResult, error) {
	msg, err := GetConsumerApi(a.Cli).viewMessageComplex(topic, msgId)
	if err != nil {
		return nil, err
	}
	if msg.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex) == "" {
		return a.consumeMessageDirectly(msg.StoreHost, group, clientId, msgId)
	} else {
		return a.consumeMessageDirectly(msg.StoreHost, group, clientId, msg.OffsetMsgId)
	}
}

func (a *MqAdmin) consumeMessageDirectly(addr, group, clientId, msgId string) (*ConsumeMessageDirectlyResult, error) {
	return GetClientApi(a.Cli).ConsumeMessageDirectly(addr, group, clientId, msgId)
}

func (a *MqAdmin) SendTopicMsg(request *SendTopicMessageRequest) (*primitive.SendResult, error) {
	msg := &primitive.Message{
		Topic: request.Topic,
		Body:  []byte(request.MessageBody),
	}
	msg.WithKeys([]string{request.Key})
	msg.WithTag(request.Tag)
	p, _ := producer.NewDefaultProducer(
		producer.WithGroupName(utils.SELF_TEST_PRODUCER_GROUP),
		producer.WithInstanceName(time.Now().String()),
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{a.Cli.GetNameSrv().AddrList()[0]})),
		producer.WithRetry(2),
	)

	err := p.Start()
	if err != nil {
		return nil, err
	}

	res, err := p.SendSync(context.Background(), msg)
	if err != nil {
		return nil, err
	}
	defer p.Shutdown()
	return res, nil
}
