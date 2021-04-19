package addons

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"text/template"

	"github.com/influxdata/toml"
	"github.com/sirupsen/logrus"
)

func TestMonitor(t *testing.T) {
	envstr := `{
	"ES_PASSWORD":"8mSJ9Rk9Hf2BA2so",
	"SELF_HOST":"10.127.3.63",
	"SELF_PORT":"9200"
	}`
	envs := make(map[string]string)
	err := json.Unmarshal([]byte(envstr), &envs)
	if err != nil {
		fmt.Println(err)
	}
	logrus.Infof("envs:%+v", envs)

	BuildTemplate(envs)
}

func BuildTemplate(envs map[string]string) error {
	data := `
		 [elasticsearch]
            servers = ['http://{{.SELF_HOST}}:{{.SELF_PORT}}']
            http_timeout = '8s'
            local = true
            cluster_health = true
            cluster_health_level = 'cluster'
            cluster_stats = false
            node_stats = ['indices', 'transport', 'http', 'jvm']
            username = 'elastic'
            password = '{{.ES_PASSWORD}}'
    `

	tpl, err := template.New("test").Parse(data)
	if err != nil {
		return err
	}

	buf := &bytes.Buffer{}
	if err := tpl.Execute(buf, envs); err != nil {
		fmt.Println(err)
		panic(err)
	}
	_, err = toml.Parse(buf.Bytes())
	if err != nil {
		logrus.Errorf("error:%v",err)
	}
	return nil
}


