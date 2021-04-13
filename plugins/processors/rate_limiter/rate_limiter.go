package ratelimiter

import (
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/ratelimit"
	"github.com/influxdata/telegraf/plugins/processors"
)

// RateLimiter .
type RateLimiter struct {
	TotalRateLimit   string                        `toml:"total_rate_limit"`
	MetricsRateLimit map[string]string             `toml:"metrics_rate_limit"`
	totalRateLimit   *ratelimit.Limiter            // 总的限流
	metricsRateLimit map[string]*ratelimit.Limiter // 针对每种指标的限流
	init             bool
}

var sampleConfig = ``

// SampleConfig .
func (p *RateLimiter) SampleConfig() string { return sampleConfig }

// Description .
func (p *RateLimiter) Description() string { return "rate limit." }

// Apply .
func (p *RateLimiter) Apply(in ...telegraf.Metric) []telegraf.Metric {
	p.initOnce()
	var result []telegraf.Metric // len(result) < len(in)，以下处理方式，避免内存分配
	for i, m := range in {
		if p.totalRateLimit != nil {
			if !p.totalRateLimit.Enter() {
				if result == nil {
					result = in[0:i]
				}
				continue
			}
		}
		if p.metricsRateLimit != nil {
			if limit, ok := p.metricsRateLimit[m.Name()]; ok {
				if !limit.Enter() {
					if result == nil {
						result = in[0:i]
					}
					continue
				}
			}
		}
		if result != nil {
			result = append(result, m)
		}
	}
	if result != nil {
		return result
	}
	return in
}

func (p *RateLimiter) initOnce() {
	if p.init {
		return
	}
	defer func() {
		p.init = true
	}()
	err := p.makeRateLimiter()
	if err != nil {
		panic(err)
	}
}

func (p *RateLimiter) makeRateLimiter() error {
	if p.MetricsRateLimit != nil && len(p.MetricsRateLimit) > 0 {
		p.metricsRateLimit = make(map[string]*ratelimit.Limiter)
		for k, v := range p.MetricsRateLimit {
			k = strings.TrimSpace(k)
			if len(k) < 0 {
				continue
			}
			v = strings.TrimSpace(v)
			if len(v) < 0 {
				continue
			}
			r, err := ratelimit.Parse(v)
			if err != nil {
				return err
			}
			p.metricsRateLimit[k] = r
		}
	}
	p.TotalRateLimit = strings.TrimSpace(p.TotalRateLimit)
	if len(p.TotalRateLimit) > 0 {
		r, err := ratelimit.Parse(p.TotalRateLimit)
		if err != nil {
			return err
		}
		p.totalRateLimit = r
	}
	return nil
}

func init() {
	processors.Add("rate_limiter", func() telegraf.Processor {
		return &RateLimiter{}
	})
}
