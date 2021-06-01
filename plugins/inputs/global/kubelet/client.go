package kubelet

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type httpClient struct {
	*http.Client
	token string
}

func (hc *httpClient) NewRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+hc.token)
	return req, nil
}

func (k *kubelet) CreateClient() *httpClient {
	c := &httpClient{}
	c.Client = &http.Client{
		Timeout: time.Second * 8,
	}

	if k.KubeletPort == readOnlyPort {
		return c
	}

	ca, err := loadCA(k.K8sTlsCa)
	if err != nil {
		log.Printf("E! loadCA failed. path: %s", k.K8sTlsCa)
		return c
	}
	c.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			RootCAs:            ca,
		},
	}

	token, err := ioutil.ReadFile(k.K8sToken)
	if err != nil {
		log.Printf("E! read token failed. path: %s", k.K8sToken)
		return c
	}
	c.token = string(token)
	return c
}

func loadCA(ca string) (*x509.CertPool, error) {
	caCert, err := ioutil.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return caCertPool, nil
}
