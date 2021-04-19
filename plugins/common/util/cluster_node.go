package util

import (
	"os"
)

var clusterNodeCache = NewMemoryCache(`{"interval":30}`)

type ClusterNode struct {
	ClusterName string `toml:"cluster_name"`
	HostIP      string `toml:"node_ip"`
	NodeType    string `toml:"node_type"`
	// NodeName    string                 `toml:"node_name"`
	Others map[string]interface{} `toml:"-"`
}

func GetClusterNodeInfo(path string) (*ClusterNode, error) {
	if os.Getenv("DICE_CLUSTER_TYPE") == "kubernetes" {
		return &ClusterNode{
			ClusterName: os.Getenv("DICE_CLUSTER_NAME"),
			HostIP:      os.Getenv("HOST_IP"),
			NodeType:    "",
			Others:      map[string]interface{}{},
		}, nil
	} else {
		hostIP := os.Getenv("HOST")
		if hostIP == "" {
			hostIP = os.Getenv("HOST_IP")
		}
		return &ClusterNode{
			ClusterName: os.Getenv("DICE_CLUSTER_NAME"),
			HostIP:      hostIP,
			NodeType:    "",
			Others:      map[string]interface{}{},
		}, nil
	}
	// memInfo := clusterNodeCache.Get(path)
	// if memInfo != nil {
	// 	return memInfo.(*ClusterNode), nil
	// }
	// lifespan := time.Minute * 10
	// info, err := ReadClusterNodeFile(path)
	// if err != nil {
	// 	lifespan = time.Minute * 1
	// 	log.Printf("fail to read cluster file, err: %s", err)
	// 	return info, err
	// }
	// clusterNodeCache.Put(path, info, lifespan)
	// return info, nil
}
