package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/tls"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Selector struct {
	Namespace     string `toml:"namespace"`
	LabelSelector string `toml:"label_selector"`
	FieldSelector string `toml:"field_selector"`
}

type Config struct {
	Mode        string           `toml:"mode"`
	URL         string           `toml:"url"`
	KubeConfig  string           `toml:"kube_config"`
	Timeout     config.Duration  `toml:"timeout"`
	BearerToken string           `toml:"bearer_token"`
	TlsConfig   tls.ClientConfig `toml:"tls_config"`
}

func NewClient(cfg Config) (*k8s.Clientset, error) {
	switch cfg.Mode {
	case "", "in_cluster":
		log.Printf("I! with InClusterConfig mode")
		return clientInClusterModel()
	case "static":
		log.Printf("I! with static config mode")
		return clientStaticModel(cfg)
	case "kube_config":
		// use the current context in kubeconfig
		cfg, err := clientcmd.BuildConfigFromFlags(cfg.URL, cfg.KubeConfig)
		if err != nil {
			return nil, fmt.Errorf("fail to build kube config: %s", err)
		}
		// create the clientset
		clientset, err := k8s.NewForConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("create k8s client err: %w", err)
		}
		return clientset, nil
	default:
		return nil, errors.New("Invalid mode " + cfg.Mode)
	}
}

func clientStaticModel(cfg Config) (*k8s.Clientset, error) {
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
	return k8s.NewForConfig(&rest.Config{
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
}

func clientInClusterModel() (*k8s.Clientset, error) {
	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return k8s.NewForConfig(clusterConfig)
}

func HealthCheck(client *k8s.Clientset, to time.Duration) error {
	ctx, _ := context.WithTimeout(context.TODO(), to)
	_, err := client.Discovery().RESTClient().Get().AbsPath("/healthz").DoRaw(ctx)
	return err
}
