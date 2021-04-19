package docker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/swarm"
	dockerpkg "github.com/docker/docker/client"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/common/util"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global"
)

// docker .
type docker struct {
	global.Base
	DockerEndpoint     string            `toml:"docker_endpoint"`
	DockerTimeout      config.Duration `toml:"docker_timeout"`
	DockerClientConfig tls.ClientConfig  `toml:"docker_client_config"`
	client             Client
	closeCh            chan struct{}
}

// Start .
func (d *docker) Start(acc telegraf.Accumulator) error { return d.initClient() }

func (d *docker) Stop() {
	if d.closeCh != nil {
		close(d.closeCh)
	}
}

func (d *docker) initClient() (err error) {
	var version = "1.21" // 1.24 is when server first started returning its version
	var defaultHeaders = map[string]string{"User-Agent": "engine-api-cli-1.0"}
	var c *dockerpkg.Client
	d.DockerEndpoint = util.Getenv(d.DockerEndpoint, "DOCKER_ENDPOINT")
	if d.DockerEndpoint == "ENV" {
		c, err = dockerpkg.NewEnvClient()
	} else {
		tlsConfig, err := d.DockerClientConfig.TLSConfig()
		if err != nil {
			return err
		}
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		httpClient := &http.Client{Transport: transport}
		c, err = dockerpkg.NewClientWithOpts(
			dockerpkg.WithHTTPHeaders(defaultHeaders),
			dockerpkg.WithHTTPClient(httpClient),
			dockerpkg.WithVersion(version),
			dockerpkg.WithHost(d.DockerEndpoint))
		if err != nil {
			return fmt.Errorf("fail to create docker client: %s", err)
		}
	}
	if err != nil {
		return fmt.Errorf("fail to create docker client: %s", err)
	}
	d.closeCh = make(chan struct{})
	cli := &client{Client: c, closeCh: d.closeCh, timeout: time.Duration(d.DockerTimeout)}
	d.client = cli
	cli.init()
	return nil
}

var instance = &docker{
	DockerTimeout: config.Duration(10 * time.Second),
}

func init() {
	inputs.Add("global_docker", func() telegraf.Input {
		return instance
	})
}

// Client .
type Client interface {
	Info(ctx context.Context) (types.Info, error)
	ContainerList(ctx context.Context, options types.ContainerListOptions) ([]types.Container, error)
	ContainerStats(ctx context.Context, containerID string, stream bool) (types.ContainerStats, error)
	ContainerInspect(ctx context.Context, containerID string) (types.ContainerJSON, error)
	ServiceList(ctx context.Context, options types.ServiceListOptions) ([]swarm.Service, error)
	TaskList(ctx context.Context, options types.TaskListOptions) ([]swarm.Task, error)
	NodeList(ctx context.Context, options types.NodeListOptions) ([]swarm.Node, error)
	Events(ctx context.Context, options types.EventsOptions) (<-chan events.Message, <-chan error)
	ImageInspectWithRaw(ctx context.Context, imageID string) (types.ImageInspect, []byte, error)
	ImageInspect(ctx context.Context, imageID string) (types.ImageInspect, []byte, error)
}

type client struct {
	*dockerpkg.Client
	timeout time.Duration

	closeCh           chan struct{}
	containers        []types.Container
	inspectCache      map[string]*inspectCacheItem
	lastGetContainers time.Time
	clock             sync.Mutex

	listener    map[int]*listener
	listenerIdx int
	elock       sync.RWMutex
	eventCh     chan struct{}
}

type listener struct {
	msg    chan events.Message
	errs   chan error
	events map[string]bool
}

type inspectCacheItem struct {
	time time.Time
	types.ContainerJSON
}

func (c *client) init() {
	c.inspectCache = make(map[string]*inspectCacheItem)
	go func() {
		tick := time.Tick(20 * time.Second)
		for {
			select {
			case <-tick:
				now := time.Now()
				c.clock.Lock()
				for id, item := range c.inspectCache {
					if item.time.Add(20 * time.Second).Before(now) {
						delete(c.inspectCache, id)
					}
				}
				c.clock.Unlock()
			case <-c.closeCh:
				return
			}
		}
	}()
}

func (c *client) ImageInspect(ctx context.Context, imageID string) (types.ImageInspect, []byte, error) {
	return c.Client.ImageInspectWithRaw(ctx, imageID)
}

func (c *client) ContainerInspect(ctx context.Context, containerID string) (types.ContainerJSON, error) {
	now := time.Now()
	c.clock.Lock()
	defer c.clock.Unlock()
	if inspect, ok := c.inspectCache[containerID]; ok {
		if inspect.time.Add(20 * time.Second).After(now) {
			return inspect.ContainerJSON, nil
		}
	}
	_, ok := ctx.Deadline()
	if !ok && int64(c.timeout) > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	inspect, err := c.Client.ContainerInspect(ctx, containerID)
	if err == nil {
		c.inspectCache[containerID] = &inspectCacheItem{
			time:          time.Now(),
			ContainerJSON: inspect,
		}
	}
	return inspect, err
}

// ContainerList 缓存 list
func (c *client) ContainerList(ctx context.Context, options types.ContainerListOptions) ([]types.Container, error) {
	c.clock.Lock()
	defer c.clock.Unlock()
	now := time.Now()
	if c.lastGetContainers.Add(20 * time.Second).After(now) {
		log.Println("I! [docker] get containers from cache")
		return c.containers, nil
	}
	containers, err := c.Client.ContainerList(ctx, options)
	if err != nil {
		return containers, err
	}
	c.containers = containers
	c.lastGetContainers = now
	return containers, nil
}

// Events 共享 docker events 事件连接
func (c *client) Events(ctx context.Context, options types.EventsOptions) (<-chan events.Message, <-chan error) {
	c.elock.Lock()
	defer c.elock.Unlock()
	if len(c.listener) <= 0 {
		c.listener = make(map[int]*listener)
		c.eventCh = make(chan struct{})
		go func() {
			defer log.Printf("I! [docker] events exit")
			var filterArgs = []filters.KeyValuePair{
				{Key: "type", Value: "container"},
				{Key: "event", Value: "die"},
				{Key: "event", Value: "destroy"},
				{Key: "event", Value: "kill"},
				{Key: "event", Value: "oom"},
				{Key: "event", Value: "stop"},
				{Key: "event", Value: "pause"},
				{Key: "event", Value: "start"},
				{Key: "event", Value: "create"},
			}
			options := types.EventsOptions{
				Filters: filters.NewArgs(filterArgs...),
				Since:   time.Now().Format(time.RFC3339),
			}
			events, errs := c.Client.Events(context.Background(), options)
			for {
				select {
				case event := <-events:
					log.Printf("I! [docker] recv docker event message: %+v", event)
					c.elock.RLock()
					for _, l := range c.listener {
						if l.events[event.Action] {
							l.msg <- event
						}
					}
					c.elock.RUnlock()
				case err := <-errs:
					c.elock.RLock()
					for _, l := range c.listener {
						l.errs <- err
					}
					c.elock.RUnlock()
					if err == io.EOF {
						return
					}
					log.Printf("W! [docker] event error: %s", err)
					return
				case <-c.eventCh:
					return
				}
			}
		}()
	}
	c.listenerIdx++
	l := &listener{
		msg:    make(chan events.Message, 2),
		errs:   make(chan error, 2),
		events: make(map[string]bool),
	}
	c.listener[c.listenerIdx] = l
	options.Filters.WalkValues("event", func(value string) error {
		l.events[value] = true
		return nil
	})
	go func(id int) {
		<-ctx.Done()
		c.elock.Lock()
		defer c.elock.Unlock()
		l := c.listener[id]
		delete(c.listener, id)
		close(l.msg)
		close(l.errs)
		if len(c.listener) <= 0 {
			close(c.eventCh)
		}
	}(c.listenerIdx)
	return l.msg, l.errs
}

// GetClient .
func GetClient() (Client, time.Duration) {
	return instance.client, time.Duration(instance.DockerTimeout)
}
