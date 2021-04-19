package terminus

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func EnvsTransform() {
	var mapper = map[string]interface{}{
		"DICE_CLUSTER_TYPE": func(key, str string) (string, string) {
			str = strings.ToLower(str)
			if str == "kubernetes" {
				return "CLUSTER_TYPE", "k8s"
			}
			return "CLUSTER_TYPE", str
		},
		"COLLECTOR_ADDR": func(key, str string) {
			isEdge := strings.ToLower(os.Getenv("DICE_IS_EDGE"))
			if isEdge == "true" {
				os.Setenv("COLLECTOR_URL", os.Getenv("COLLECTOR_PUBLIC_URL"))
			} else {
				os.Setenv("COLLECTOR_URL", fmt.Sprintf("http://%s", str))
			}
		},
		"DICE_STORAGE_MOUNTPOINT": func(string, str string) (string, string) {
			return "STORAGE_MOUNT_PATH", fmt.Sprintf("['%s']", str)
		},
		"EVENTBOX_ADDR": func(key, addr string) (string, string) {
			return "EVENT_BOX_ADDR", fmt.Sprintf("http://%s/api/dice/eventbox/message/create", addr)
		},
		"MYSQL_HOST": func(key, str string) {
			// MYSQL_SERVERS: '[<%if $.MainPlatform%><%else%>''<%$.Platform.MySQL.Username%>:<%$.Platform.MySQL.Password%>@tcp(<%$.Platform.MySQL.Host%>:<%$.Platform.MySQL.Port%>)/<%$.Platform.MySQL.DiceDB%>''<%end%>]'
			isEdge := strings.ToLower(os.Getenv("DICE_IS_EDGE"))
			if isEdge == "true" {
				return
			}
			mysql := fmt.Sprintf("['%s:%s@tcp(%s:%s)/%s']",
				os.Getenv("MYSQL_USERNAME"),
				os.Getenv("MYSQL_PASSWORD"),
				os.Getenv("MYSQL_HOST"),
				os.Getenv("MYSQL_PORT"),
				os.Getenv("MYSQL_DATABASE"))
			os.Setenv("MYSQL_SERVERS", mysql)
		},
		"ES_URL": func(key, str string) {
			// ELASTICSEARCH_SERVERS: '[<%if $.MainPlatform%><%else%>''http://es1.marathon.l4lb.thisdcos.directory:9200'',''http://es2.marathon.l4lb.thisdcos.directory:9200'',''http://es3.marathon.l4lb.thisdcos.directory:9200''<%end%>]'
			// ELASTICSEARCH_GATHER_LOCAL: "true"
			isEdge := strings.ToLower(os.Getenv("DICE_IS_EDGE"))
			if isEdge == "true" {
				return
			}
			urls := strings.Split(str, ",")
			securityEnable := strings.ToLower(os.Getenv("ES_SECURITY_ENABLE"))
			if securityEnable == "true" {
				username := os.Getenv("ES_SECURITY_USERNAME")
				password := os.Getenv("ES_SECURITY_PASSWORD")
				os.Setenv("ES_USERNAME", username)
				os.Setenv("ES_PASSWORD", password)
				for i, item := range urls {
					u, err := url.Parse(item)
					if err != nil {
						continue
					}
					// u.User = url.UserPassword(username, password)
					urls[i] = u.String()
					// http://username:password@host:port
					// urls[i] = fmt.Sprintf("%s://%s:%s@%s", u.Scheme, username, password, u.Host)
				}
			}
			if len(urls) <= 1 {
				os.Setenv("ELASTICSEARCH_GATHER_LOCAL", "false")
			} else {
				os.Setenv("ELASTICSEARCH_GATHER_LOCAL", "true")
			}
			os.Setenv("ELASTICSEARCH_SERVERS", "['"+strings.Join(urls, "','")+"']")
		},
		"ZOOKEEPER_ADDR": func(key, str string) (string, string) {
			// ZOOKEEPER_SERVERS: '[''master.mesos:2181'']'
			addrs := strings.Split(str, ",")
			return "ZOOKEEPER_SERVERS", "['" + strings.Join(addrs, "','") + "']"
		},
		"CASSANDRA_ADDR": func(key, str string) (string, string) {
			jport := os.Getenv("CASSANDRA_JOLOKIA_PORT")
			if jport == "" {
				os.Setenv("CASSANDRA_JOLOKIA_PORT", "8778")
			}
			idx := strings.Index(str, ".")
			if idx > 0 {
				os.Setenv("CASSANDRA_EXPORTER_JMX_ADDR", str[:idx]+"-exporter-jmx"+str[idx:])
			}

			// CASSANDRA_JOLOKIA_URLS: '[<%if $.MainPlatform%><%else%>''http://<%$.Platform.AssignNodes.cassandra1%>:7070/jolokia'',''http://<%$.Platform.AssignNodes.cassandra2%>:7070/jolokia'',''http://<%$.Platform.AssignNodes.cassandra3%>:7070/jolokia''<%end%>]'
			// addrs := strings.Split(str, ",")
			// for i, addr := range addrs {
			// 	addrs[i] = fmt.Sprintf("http://%s:7070/jolokia", addr)
			// }
			// return "CASSANDRA_JOLOKIA_URLS", "['" + strings.Join(addrs, "','") + "']"
			// TODO 等CASSANDRA加上jolokia 后再加上
			return "CASSANDRA_JOLOKIA_URLS", "[]"
		},
		"REDIS_SENTINELS": func(key, str string) (string, string) {
			// REDIS_SERVERS: '[<%if $.MainPlatform%><%else%>''tcp://:iEVII6LY6Ebrc4HX@redis-master.marathon.l4lb.thisdcos.directory:6379''<%end%>]'
			isEdge := strings.ToLower(os.Getenv("DICE_IS_EDGE"))
			if isEdge == "true" {
				return "", ""
			}
			// TODO 暂时跳过
			return "REDIS_SERVERS", "[]"
		},
		"MASTER_MONITOR_ADDR": func(key string, str string) {
			if os.Getenv("DICE_CLUSTER_TYPE") == "dcos" {
				// MASTER_MONITOR_ADDR: <node_ip>:5050
				// MASTERS: '[<%range $node := $.Nodes%><%if eq $node.Type "master"%>''<%$node.IP%>'',<%end%><%end%>'''']'
				var masterHealthPort, unitsHealthPort = 5050, 80
				addr := strings.Split(os.Getenv("MASTER_ADDR"), ",")[0]
				if addr != "" {
					idx := strings.LastIndex(addr, ":")
					if idx >= 0 {
						port, err := strconv.Atoi(addr[idx+1:])
						if err == nil {
							unitsHealthPort = port
						}
					}
				}
				addr = strings.Split(os.Getenv("MASTER_MONITOR_ADDR"), ",")[0]
				if addr != "" {
					idx := strings.LastIndex(addr, ":")
					if idx >= 0 {
						port, err := strconv.Atoi(addr[idx+1:])
						if err == nil {
							masterHealthPort = port
						}
					} else {
						masterHealthPort = 80
					}
				}
				addrs := strings.Split(str, ",")
				for i, addr := range addrs {
					idx := strings.Index(addr, ":")
					if idx > 0 {
						addrs[i] = addr[0:idx]
					}
				}
				os.Setenv("MASTERS", "['"+strings.Join(addrs, "','")+"']")
				os.Setenv("MASTER_HEALTH_PORT", fmt.Sprint(masterHealthPort))
				os.Setenv("SLAVE_HEALTH_PORT", fmt.Sprint(masterHealthPort+1))
				os.Setenv("UNITS_HEALTH_PORT", fmt.Sprint(unitsHealthPort))
			}
		},
		// "MASTER_MONITOR_URL": func(key string, str string) {
		// 	// K8S_URLS: '[<%range $node := $.Nodes%><%if eq $node.Type "master"%>''http://<%$node.IP%>:8080'',<%end%><%end%>'''']'
		// 	// urls := strings.Split(str, ",")
		// 	// os.Setenv("K8S_URLS", "['"+strings.Join(urls, "','")+"']")
		// 	// TODO，证书访问的方式后面会变，待确定
		// 	// os.Setenv("K8S_TOKEN_FILE", "")
		// 	// os.Setenv("K8S_CA_FILE", "")
		// },
		"LB_MONITOR_URL": func(key, str string) {
			// LB_MONITOR_URL: http://<lb_node_ip>:9090
			// HAPROXY_SERVERS: '[<%range $node := $.Nodes%><%if eq $node.Type "lb"%>''http://<%$node.IP%>:9090/haproxy?stats'',<%end%><%end%>'''']'
			if os.Getenv("DICE_CLUSTER_TYPE") == "dcos" {
				urls := strings.Split(str, ",")
				for i, url := range urls {
					urls[i] = fmt.Sprintf("'%s/haproxy?stats'", url)
				}
				os.Setenv("HAPROXY_SERVERS", "["+strings.Join(urls, ",")+"]")
			}
		},
		// "MASTER_VIP_URL": func(key, str string) (string, string) {
		// 	// http://insecure-kubernetes.default.svc.cluster.local:80
		// 	// os.Setenv("K8S_URLS", "['"+strings.Join(urls, "','")+"']")
		// 	return "KUBERNETES_URL", str
		// },
		"BOOTSTRAP_SERVERS": func(key, str string) (string, string) {
			// // addon-kafka-i1.default.svc.cluster.local:9092,addon-kafka-i2.default.svc.cluster.local:9092,addon-kafka-i3.default.svc.cluster.local:9092
			addrs := strings.Split(str, ",")
			jport := os.Getenv("KAFKA_JOLOKIA_PORT")
			if jport == "" {
				jport = "8778"
			}
			for i, addr := range addrs {
				host, _, _ := net.SplitHostPort(addr)
				addrs[i] = fmt.Sprintf("'http://%s/jolokia'", net.JoinHostPort(host, jport))
			}
			return "KAFKA_JOLOKIA_URLS", "[" + strings.Join(addrs, ",") + "]"
		},
		"ETCD_ENDPOINTS": func(key string, str string) {
			urls := strings.Split(str, ",")
			for i, url := range urls {
				urls[i] = fmt.Sprintf("'%s'", url)
			}
			os.Setenv("ETCD_URLS", "["+strings.Join(urls, ",")+"]")
		},
	}
	for k, nk := range mapper {
		v := os.Getenv(k)
		switch val := nk.(type) {
		case string:
			os.Setenv(k, val)
		case func(string, string) (string, string):
			nk, nv := val(k, v)
			os.Setenv(nk, nv)
		case func(string) string:
			nv := val(v)
			os.Setenv(k, nv)
		case func(string, string):
			val(k, v)
		}
	}
	if os.Getenv("ETCD_URLS") == "" {
		os.Setenv("ETCD_URLS", "[]")
	}
	fmt.Println("envs: \n", strings.Join(os.Environ(), "\n"))
}
