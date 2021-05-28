package kubernetes

import (
	"context"
)

type Selector struct {
	Namespace     string `toml:"namespace"`
	LabelSelector string `toml:"label_selector"`
	FieldSelector string `toml:"field_selector"`
}

type Watcher interface {
	Watch(ctx context.Context, ch chan<- *Item)
}