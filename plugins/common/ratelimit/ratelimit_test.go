package ratelimit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRateLimitParse(t *testing.T) {
	r, err := Parse("5/s, 100")
	assert.Nil(t, err)
	assert.Equal(t, float64(5), float64(r.Limit()), "limit not correct")
	assert.Equal(t, int(100), r.Burst(), "bucket not correct")

	r, err = Parse("5/s")
	assert.Nil(t, err)
	assert.Equal(t, float64(5), float64(r.Limit()), "limit not correct")
	assert.Equal(t, int(5), r.Burst(), "bucket not correct")

	r, err = Parse("0.5/s")
	assert.Nil(t, err)
	assert.Equal(t, float64(0.5), float64(r.Limit()), "limit not correct")
	assert.Equal(t, int(1), r.Burst(), "bucket not correct")

	r, err = Parse("10.2/s")
	assert.Nil(t, err)
	assert.Equal(t, float64(10.2), float64(r.Limit()), "limit not correct")
	assert.Equal(t, int(10), r.Burst(), "bucket not correct")

	r, err = Parse("100.2/s, 200")
	assert.Nil(t, err)
	assert.Equal(t, float64(100.2), float64(r.Limit()), "limit not correct")
	assert.Equal(t, int(200), r.Burst(), "bucket not correct")

	r, err = Parse("100.2, 200")
	assert.Nil(t, err)
	assert.Equal(t, float64(100.2), float64(r.Limit()), "limit not correct")
	assert.Equal(t, int(200), r.Burst(), "bucket not correct")

	r, err = Parse("100xx, 200")
	assert.NotNil(t, err)

	r, err = Parse("100, xxx")
	assert.NotNil(t, err)

	r, err = Parse("10/s, 100, wait")
	assert.Nil(t, err)
	assert.Equal(t, r.mode, LimitModeWait, "mode not correct")

	r, err = Parse("10/s, 100, discard")
	assert.Nil(t, err)
	assert.Equal(t, r.mode, LimitModeDiscard, "mode not correct")
}
