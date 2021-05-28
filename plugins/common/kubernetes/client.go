package kubernetes

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/tls"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Config struct {
	URL         string           `toml:"url"`
	Timeout     config.Duration  `toml:"timeout"`
	BearerToken string           `toml:"bearer_token"`
	TlsConfig   tls.ClientConfig `toml:"tls_config"`
}

func NewClient(cfg Config) (*k8s.Clientset, error) {
	var client *k8s.Clientset
	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Printf("I! unabled to do with InClusterConfig - %v. try with static config...", err)
		var ca, cert, key, token string
		if len(cfg.TlsConfig.TLSCA) > 0 {
			_, err := os.Stat(cfg.TlsConfig.TLSCA)
			if err == nil {
				ca = cfg.TlsConfig.TLSCA
			}
		}
		if len(cfg.TlsConfig.TLSCert) > 0 {
			_, err := os.Stat(cfg.TlsConfig.TLSCert)
			if err == nil {
				cert = cfg.TlsConfig.TLSCert
			}
		}
		if len(cfg.TlsConfig.TLSKey) > 0 {
			_, err := os.Stat(cfg.TlsConfig.TLSKey)
			if err == nil {
				key = cfg.TlsConfig.TLSKey
			}
		}
		if len(cfg.BearerToken) > 0 {
			_, err := os.Stat(cfg.BearerToken)
			if err == nil {
				token = cfg.BearerToken
			}
		}
		c, err := k8s.NewForConfig(&rest.Config{
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: cfg.TlsConfig.InsecureSkipVerify,
				CAFile:   ca,
				CertFile: cert,
				KeyFile:  key,
			},
			Host:          cfg.URL,
			BearerToken:   token,
			ContentConfig: rest.ContentConfig{},
		})
		if err != nil {
			return nil, err
		}

		client = c
	} else {
		c, err := k8s.NewForConfig(clusterConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to NewForConfig with InClusterConfig. err %s", err)
		}

		client = c
	}
	if !HealthCheck(client) {
		return nil, fmt.Errorf("try to connect apiserver failed")
	}
	return client, nil
}

func HealthCheck(client *k8s.Clientset) bool {
	_, err := client.Discovery().RESTClient().Get().AbsPath("/healthz").DoRaw(context.TODO())
	if err != nil {
		return false
	}
	return true
}
