package admin

import "errors"

/**
 * ***集群相关操作接口
 */
func (a *MqAdmin) List() (map[string]interface{}, error) {
	cluster, err := GetClientApi(a.Cli).GetBrokerClusterInfo()
	if err != nil {
		return nil, err
	}

	if cluster == nil || len(cluster.BrokerAddrTable) == 0 {
		return nil, errors.New("集群配置数据查询失败")
	}
	brokerServer := make(map[string]map[int64]interface{})
	for _, brokerData := range cluster.BrokerAddrTable {
		masterSlaveMap := make(map[int64]interface{})
		for key, addr := range brokerData.BrokerAddresses {
			kv, err := GetClientApi(a.Cli).getBrokerRuntimeInfo(addr)
			if err != nil {
				continue
			}
			if kv == nil {
				continue
			}
			masterSlaveMap[key] = kv.Table
		}
		brokerServer[brokerData.BrokerName] = masterSlaveMap
	}
	data := make(map[string]interface{})
	data["clusterInfo"] = cluster
	data["brokerServer"] = brokerServer
	return data, nil
}

func (a *MqAdmin) BrokerConfig(addr string) (*map[string]string, error) {
	return GetClientApi(a.Cli).getBrokerConfig(addr)
}
