package kubelet

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkName(b *testing.B) {
	data, err := ioutil.ReadFile("xxx.json")
	assert.Nil(b, err)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		marshalobj(bytes.NewReader(data))
	}
}

func marshalobj(r io.Reader)  {
	stats := StatusSummaryResp{}
	json.NewDecoder(r).Decode(&stats)
}
