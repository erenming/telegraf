package global

import "github.com/influxdata/telegraf"

// Base .
type Base struct{}

// SampleConfig .
func (*Base) SampleConfig() string { return `` }

// Description .
func (*Base) Description() string { return `` }

// Gather .
func (*Base) Gather(acc telegraf.Accumulator) error { return nil }

// Start .
func (*Base) Start(acc telegraf.Accumulator) error { return nil }

// Stop .
func (*Base) Stop() {}
