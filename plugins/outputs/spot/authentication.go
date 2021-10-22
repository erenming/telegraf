package spot

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
)

const (
	authTypeKey      = "key"
	authTypeBasic    = "basic"
	authCfgUserName  = "username"
	authCfgPassword  = "password"
	authCfgAccessKey = "telegraf_access_key"
	erdaClusterKey   = "X-Erda-Cluster-Key"
)

var (
	clusterCredentialToken string
	lock                   sync.Mutex
)

type authConfig struct {
	Type     string            `toml:"type"`
	Property map[string]string `toml:"property"`
}

type Option func(authentication *Authentication)

type Authentication struct {
	cfg        *authConfig
	clusterKey string
}

func NewAuthentication(opts ...Option) *Authentication {
	a := Authentication{}

	for _, opt := range opts {
		opt(&a)
	}

	return &a
}

func WithAuthConfig(c *authConfig) Option {
	return func(a *Authentication) {
		a.cfg = c
	}
}

func WithClusterKey(c string) Option {
	return func(a *Authentication) {
		a.clusterKey = c
	}
}

func (a *Authentication) Secure(req *http.Request) {
	switch a.cfg.Type {
	case authTypeKey:
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", readToken()))
		req.Header.Set(erdaClusterKey, a.clusterKey)
	case authTypeBasic:
		req.SetBasicAuth(a.cfg.Property[authCfgUserName], a.cfg.Property[authCfgPassword])
	}
}

func readToken() string {
	return clusterCredentialToken
}

func setToken(token []byte) {
	lock.Lock()
	defer lock.Unlock()
	if len(token) == 0 {
		return
	}

	clusterCredentialToken = strings.Replace(string(token), "\n", "", -1)
}
