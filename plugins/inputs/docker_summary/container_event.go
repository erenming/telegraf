package dockersummary

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/influxdata/telegraf"
	gdocker "github.com/influxdata/telegraf/plugins/inputs/global/docker"
)

func (s *Summary) startDockerEventLoop(acc telegraf.Accumulator) error {
	log.Printf("I! [docker_summary] start event loop")
	go func(acc telegraf.Accumulator) error {
		var filterArgs = []filters.KeyValuePair{
			{Key: "type", Value: "container"},
			{Key: "event", Value: "die"},
			{Key: "event", Value: "kill"},
			{Key: "event", Value: "oom"},
			{Key: "event", Value: "stop"},
			{Key: "event", Value: "pause"},
		}
		options := types.EventsOptions{
			Filters: filters.NewArgs(filterArgs...),
			Since:   time.Now().Format(time.RFC3339),
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client, _ := gdocker.GetClient()
		events, errs := client.Events(ctx, options)
		for {
			select {
			case event := <-events:
				log.Printf("I! [docker_summary] recv docker event message: %+v", event)
				if err := s.gatherContainerFromEvent(event, acc); err != nil {
					log.Printf(
						"W! [docker_summary] failed to handle docker event, id: %s, type: %s, action: %s, error: %s",
						event.ID, event.Type, event.Action, err)
				}
			case err := <-errs:
				if err == io.EOF {
					return nil
				}
				log.Printf("W! [docker_summary] event error: %s", err)
				return err
			}
		}
	}(acc)
	return nil
}

func (s *Summary) gatherContainerFromEvent(event events.Message, acc telegraf.Accumulator) error {
	if !s.initialize {
		return nil
	}
	return s.gatherContainer(event.ID, nil, acc)
}
