package utils

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	ROCKETMQ_HOME_ENV           = "ROCKETMQ_HOME"
	ROCKETMQ_HOME_PROPERTY      = "rocketmq.home.dir"
	NAMESRV_ADDR_ENV            = "NAMESRV_ADDR"
	NAMESRV_ADDR_PROPERTY       = "rocketmq.namesrv.addr"
	MESSAGE_COMPRESS_LEVEL      = "rocketmq.message.compressLevel"
	DEFAULT_NAMESRV_ADDR_LOOKUP = "jmenv.tbsite.net"
	DEFAULT_TOPIC               = "TBW102"
	BENCHMARK_TOPIC             = "BenchmarkTest"
	DEFAULT_PRODUCER_GROUP      = "DEFAULT_PRODUCER"
	DEFAULT_CONSUMER_GROUP      = "DEFAULT_CONSUMER"
	TOOLS_CONSUMER_GROUP        = "TOOLS_CONSUMER"
	FILTERSRV_CONSUMER_GROUP    = "FILTERSRV_CONSUMER"
	MONITOR_CONSUMER_GROUP      = "__MONITOR_CONSUMER"
	CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER"
	SELF_TEST_PRODUCER_GROUP    = "SELF_TEST_P_GROUP"
	SELF_TEST_CONSUMER_GROUP    = "SELF_TEST_C_GROUP"
	SELF_TEST_TOPIC             = "SELF_TEST_TOPIC"
	OFFSET_MOVED_EVENT          = "OFFSET_MOVED_EVENT"
	ONS_HTTP_PROXY_GROUP        = "CID_ONS-HTTP-PROXY"
	CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION"
	CID_ONSAPI_OWNER_GROUP      = "CID_ONSAPI_OWNER"
	CID_ONSAPI_PULL_GROUP       = "CID_ONSAPI_PULL"
	CID_RMQ_SYS_PREFIX          = "CID_RMQ_SYS_"
	DEFAULT_CHARSET             = "UTF-8"
	MASTER_ID                   = 0
	RETRY_GROUP_TOPIC_PREFIX    = "%RETRY%"
	DLQ_GROUP_TOPIC_PREFIX      = "%DLQ%"
	SYSTEM_TOPIC_PREFIX         = "rmq_sys_"
	UNIQUE_MSG_QUERY_FLAG       = "_UNIQUE_KEY_QUERY"
	DEFAULT_TRACE_REGION_ID     = "DefaultRegion"
	CONSUME_CONTEXT_TYPE        = "ConsumeContextType"
)

type MixAll struct {
}

var MixAllUtil = &MixAll{}

func (u *MixAll) GetRetryTopic(consumerGroup string) string {
	return RETRY_GROUP_TOPIC_PREFIX + consumerGroup
}

func (u *MixAll) GetDLQTopic(consumerGroup string) string {
	return DLQ_GROUP_TOPIC_PREFIX + consumerGroup
}

func (u *MixAll) BrokerVIPChannel(isChange bool, brokerAddr string) string {
	if isChange {
		ipAndPort := strings.Split(brokerAddr, ":")
		port, _ := strconv.Atoi(ipAndPort[1])
		return fmt.Sprintf("%s:%s", ipAndPort[0], (port - 2))
	} else {
		return brokerAddr
	}
}

func (u *MixAll) GetPID() int {
	return os.Getpid()
}

func (u *MixAll) IsSysConsumerGroup(consumerGroup string) bool {
	return strings.HasPrefix(consumerGroup, CID_RMQ_SYS_PREFIX)
}
func (u *MixAll) IsSystemTopic(topic string) bool {
	return strings.HasPrefix(topic, SYSTEM_TOPIC_PREFIX)
}

func localhostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Println("主机名称获取失败：" + err.Error())
	}
	return hostname
}

func (u *MixAll) StringToProperties(str string) map[string]string {
	properties := make(map[string]string)

	lines := strings.Split(str, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		index := strings.Index(line, "=")
		if index == -1 {
			index = strings.Index(line, ":")
		}

		if index != -1 {
			key := strings.TrimSpace(line[:index])
			value := strings.TrimSpace(line[index+1:])
			properties[key] = value
		}
	}

	return properties
}
