package kubernetes

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global"
	"k8s.io/client-go/rest"

	k8s "k8s.io/client-go/kubernetes"
)

// kubernetes .
type kubernetes struct {
	global.Base
	K8sURL          string           `toml:"k8s_url"`
	K8sTimeout      config.Duration  `toml:"k8s_timeout"`
	K8sBearerToken  string           `toml:"k8s_bearer_token"`
	K8sClientConfig tls.ClientConfig `toml:"k8s_client_config"`
	client          *k8s.Clientset
}

// Start .
func (k *kubernetes) Start(acc telegraf.Accumulator) error { return k.initClient() }

func (k *kubernetes) initClient() error {
	var ca, cert, key, token string
	if len(k.K8sClientConfig.TLSCA) > 0 {
		_, err := os.Stat(k.K8sClientConfig.TLSCA)
		if err == nil {
			ca = k.K8sClientConfig.TLSCA
		}
	}
	if len(k.K8sClientConfig.TLSCert) > 0 {
		_, err := os.Stat(k.K8sClientConfig.TLSCert)
		if err == nil {
			cert = k.K8sClientConfig.TLSCert
		}
	}
	if len(k.K8sClientConfig.TLSKey) > 0 {
		_, err := os.Stat(k.K8sClientConfig.TLSKey)
		if err == nil {
			key = k.K8sClientConfig.TLSKey
		}
	}
	if len(k.K8sBearerToken) > 0 {
		_, err := os.Stat(k.K8sBearerToken)
		if err == nil {
			token = k.K8sBearerToken
		}
	}

	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Printf("I! unabled to do with InClusterConfig - %v. try with static config", err)
		c, err := k8s.NewForConfig(&rest.Config{
			TLSClientConfig: rest.TLSClientConfig{
				ServerName: k.K8sURL,
				Insecure:   k.K8sClientConfig.InsecureSkipVerify,
				CAFile:     ca,
				CertFile:   cert,
				KeyFile:    key,
			},
			BearerToken:   token,
			ContentConfig: rest.ContentConfig{},
		})
		if err != nil {
			return err
		}

		k.client = c
	} else {
		c, err := k8s.NewForConfig(clusterConfig)
		if err != nil {
			return fmt.Errorf("E! failed to NewForConfig with InClusterConfig. err %s", err)
		}

		k.client = c
	}

	log.Printf("create k8s client success!!!")
	return nil
}

var instance = &kubernetes{
	K8sTimeout: config.Duration(20 * time.Second),
}

func init() {
	inputs.Add("global_kubernetes", func() telegraf.Input {
		return instance
	})
}

// GetClient .
func GetClient() (*k8s.Clientset, time.Duration) {
	return instance.client, time.Duration(instance.K8sTimeout)
}

