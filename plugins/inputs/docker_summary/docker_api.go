package dockersummary

import (
	"context"
	"fmt"

	"github.com/docker/docker/api/types"
	gdocker "github.com/influxdata/telegraf/plugins/inputs/global/docker"
)

func (s *Summary) dockerInspect(id string) (*types.ContainerJSON, error) {
	client, timeout := gdocker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	info, err := client.ContainerInspect(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("Error inspecting docker container: %s", err.Error())
	}
	return &info, nil
}

func (s *Summary) dockerStats(id string) (*types.ContainerStats, error) {
	client, timeout := gdocker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stats, err := client.ContainerStats(ctx, id, false)
	if err != nil {
		return nil, fmt.Errorf("Error getting docker stats: %s", err.Error())
	}
	return &stats, nil
}

func (s *Summary) dockerList(opt types.ContainerListOptions) ([]types.Container, error) {
	client, timeout := gdocker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	list, err := client.ContainerList(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("Error getting docker container list: %s", err.Error())
	}
	return list, nil
}

func (s *Summary) dockerImageInspect(imageID string) (*types.ImageInspect, error) {
	client, timeout := gdocker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	inspect, _, err := client.ImageInspectWithRaw(ctx, imageID)
	if err != nil {
		return nil, fmt.Errorf("Error inspecting docker images: %s", err.Error())
	}
	return &inspect, nil
}
