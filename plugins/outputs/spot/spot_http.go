package spot

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/common/watcher"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)

var sampleConfig = `
  ## URL is the address to send metrics to
  url = "http://127.0.0.1:8080/metric"

  ## Timeout for HTTP message
  # timeout = "5s"

  ## HTTP method, one of: "POST" or "PUT"
  # method = "POST"

  ## HTTP Basic Auth credentials
  # username = "username"
  # password = "pa$$word"

  ## OAuth2 Client Credentials Grant
  # client_id = "clientid"
  # client_secret = "secret"
  # token_url = "https://indentityprovider/oauth2/v1/token"
  # scopes = ["urn:opc:idm:__myscopes__"]

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## Data format to output.
  ## Each data format has it's own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  # data_format = "influx"

  ## Additional HTTP headers
  # [outputs.http.headers]
  #   # Should be set manually to "application/json" for json data_format
  #   Content-Type = "text/plain; charset=utf-8"
`

const (
	defaultClientTimeout = 5 * time.Second
	defaultContentType   = "text/plain; charset=utf-8"
	defaultMethod        = http.MethodPost
)

type HTTP struct {
	URL                   string            `toml:"url"`
	Timeout               config.Duration   `toml:"timeout"`
	Method                string            `toml:"method"`
	Headers               map[string]string `toml:"headers"`
	ClientID              string            `toml:"client_id"`
	ClientSecret          string            `toml:"client_secret"`
	TokenURL              string            `toml:"token_url"`
	Scopes                []string          `toml:"scopes"`
	Includes              []string          `toml:"includes"`
	Excludes              []string          `toml:"excludes"`
	ContentEncoding       string            `toml:"content_encoding"`
	CustomContentEncoding string            `toml:"custom_content_encoding"`
	ClusterKey            string            `toml:"cluster_key"`
	AuthConfig            *authConfig       `toml:"auth"`

	tls.ClientConfig

	client     *http.Client
	serializer serializers.Serializer
	auth       *Authentication
}

func (h *HTTP) SetSerializer(serializer serializers.Serializer) {
	h.serializer = serializer
}

func (h *HTTP) createClient(ctx context.Context) (*http.Client, error) {
	tlsCfg, err := h.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsCfg,
			Proxy:           http.ProxyFromEnvironment,
		},
		Timeout: time.Duration(h.Timeout),
	}

	if h.ClientID != "" && h.ClientSecret != "" && h.TokenURL != "" {
		oauthConfig := clientcredentials.Config{
			ClientID:     h.ClientID,
			ClientSecret: h.ClientSecret,
			TokenURL:     h.TokenURL,
			Scopes:       h.Scopes,
		}
		ctx = context.WithValue(ctx, oauth2.HTTPClient, client)
		client = oauthConfig.Client(ctx)
	}

	return client, nil
}

func (h *HTTP) Connect() error {
	if h.Method == "" {
		h.Method = http.MethodPost
	}
	h.Method = strings.ToUpper(h.Method)
	if h.Method != http.MethodPost && h.Method != http.MethodPut {
		return fmt.Errorf("invalid method [%s] %s", h.URL, h.Method)
	}

	if h.Timeout == 0 {
		h.Timeout = config.Duration(defaultClientTimeout)
	}

	ctx := context.Background()
	client, err := h.createClient(ctx)
	if err != nil {
		return err
	}

	h.client = client

	h.auth = NewAuthentication(
		WithAuthConfig(h.AuthConfig),
		WithClusterKey(h.ClusterKey),
	)

	// Init credential and file_watcher
	// If auth type is key and specified accessKey, doesn't start credential watcher.
	// Credential information changes cannot be perception.
	if h.AuthConfig.Type == authTypeKey {
		if h.AuthConfig.Property[authCfgAccessKey] != "" {
			setToken([]byte(h.AuthConfig.Property[authCfgAccessKey]))
		} else {
			// Init credential info
			content, err := ioutil.ReadFile(watcher.ClusterCredentialFullPath)
			if err != nil {
				log.Printf("init read credential info error: %v", err)
				return err
			}

			log.Printf("I! read init content: %v", string(content))
			setToken(content)

			// Start file watcher
			if _, err = watcher.NewFileWatcher(func(e fsnotify.Event) {
				res, err := ioutil.ReadFile(watcher.ClusterCredentialFullPath)
				if err != nil {
					log.Printf("E! read cluster credential error: %v", err)
					return
				}
				log.Printf("I! get new credential content: %s", string(res))
				setToken(res)
			}); err != nil {
				log.Printf("E! new credential file watcher error: %v", err)
				return err
			}
		}
	}

	return nil
}

func (h *HTTP) Close() error {
	return nil
}

func (h *HTTP) Description() string {
	return "A plugin that can transmit metrics over HTTP"
}

func (h *HTTP) SampleConfig() string {
	return sampleConfig
}

func (h *HTTP) Write(metrics []telegraf.Metric) error {

	if len(metrics) == 0 {
		return nil
	}

	start := time.Now()

	reqBody, err := h.serializer.SerializeBatch(metrics)
	if err != nil {
		return err
	}

	if len(reqBody) == 0 {
		return nil
	}

	serializeEnds := time.Since(start)

	base64Starts := time.Now()

	enc := base64.StdEncoding

	base64Body := make([]byte, enc.EncodedLen(len(reqBody)))

	enc.Encode(base64Body, reqBody)

	base64Ends := time.Since(base64Starts)

	gzipStart := time.Now()

	var reqBodyBuffer io.Reader = bytes.NewBuffer(base64Body)

	if h.ContentEncoding == "gzip" {
		reqBodyBuffer, err = internal.CompressWithGzip(reqBodyBuffer)
		if err != nil {
			return err
		}
	}

	gzipEnd := time.Since(gzipStart)

	requestID := uuid.New().String()

	httpWriteStart := time.Now()

	if err := h.write(requestID, reqBodyBuffer); err != nil {
		return err
	}

	httpWriteEnd := time.Since(httpWriteStart)

	log.Printf("I! Send %d metrics to %s. request-id: %s encoding: %s cost: %.4fs  serialize: %.4fs base64: %.4fs  gzip: %.4fs  http write: %.4fs  ", len(metrics), h.URL, requestID,
		h.CustomContentEncoding,
		time.Since(start).Seconds(), serializeEnds.Seconds(), base64Ends.Seconds(), gzipEnd.Seconds(), httpWriteEnd.Seconds())

	return nil
}

func (h *HTTP) write(requestID string, reqBody io.Reader) error {
	req, err := http.NewRequest(h.Method, h.URL, reqBody)
	if err != nil {
		return err
	}

	h.auth.Secure(req)

	req.Header.Set("terminus-request-id", requestID)
	req.Header.Set("Content-Type", defaultContentType)
	if h.ContentEncoding == "gzip" {
		req.Header.Set("Content-Encoding", "gzip")
	}
	req.Header.Set("Custom-Content-Encoding", h.CustomContentEncoding)
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("W! close response body error %s", err)
		}
	}()
	// defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	log.Printf("I! push data response:%d %s ", resp.StatusCode, data)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("when writing to [%s] received status code: %d", h.URL, resp.StatusCode)
	}

	return nil
}

func init() {
	outputs.Add("spot", func() telegraf.Output {
		return &HTTP{
			Timeout:               config.Duration(defaultClientTimeout),
			Method:                defaultMethod,
			CustomContentEncoding: "base64",
		}
	})
}
