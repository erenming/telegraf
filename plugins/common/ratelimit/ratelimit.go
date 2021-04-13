package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/time/rate"
)

type Limiter struct {
	*rate.Limiter
	mode string // wait、discard， default is discard
}

// Enter .
func (l *Limiter) Enter() bool {
	if l.mode == LimitModeWait {
		l.Wait(context.Background())
		return true
	}
	// l.mode == LimitModeDiscard
	if !l.Allow() {
		return false // 速度太快，丢弃
	}
	return true
}

const (
	LimitModeWait    = "wait"
	LimitModeDiscard = "discard"
)

func Parse(text string) (*Limiter, error) {
	parts := strings.SplitN(text, ",", 3)
	parts[0] = strings.TrimSpace(parts[0])
	parts[0] = strings.TrimSuffix(parts[0], "/s")
	limit, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return nil, fmt.Errorf("fail to parse rate limit: %s", err)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("invalid limit value %s", text)
	}
	var bucket int
	if len(parts) > 1 {
		parts[1] = strings.TrimSpace(parts[1])
		if len(parts[1]) > 0 {
			v, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, fmt.Errorf("fail to parse rate limit bucket: %s", err)
			}
			if v <= 0 {
				return nil, fmt.Errorf("invalid rate limit bucket")
			}
			bucket = v
		}
	}
	if bucket <= 0 {
		if limit <= 1 {
			bucket = 1
		} else {
			bucket = int(limit)
		}
	}
	var mode = LimitModeDiscard
	if len(parts) > 2 {
		parts[2] = strings.TrimSpace(parts[2])
		if len(parts[2]) > 0 {
			switch parts[2] {
			case LimitModeWait:
				mode = LimitModeWait
			case LimitModeDiscard:
				mode = LimitModeDiscard
			default:
				return nil, fmt.Errorf("invalid rate limit mode: %s", parts[2])
			}
		}
	}
	return &Limiter{rate.NewLimiter(rate.Limit(limit), bucket), mode}, nil
}
