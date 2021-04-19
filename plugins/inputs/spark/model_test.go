package spark

import (
	"testing"
)

func TestUnmarshalJvm(t *testing.T) {
	data := map[string]map[string]interface{}{
		"local-1589262777037.driver.jvm.PS-MarkSweep.time": {
			"value": 42,
		},
		"local-1589262777037.driver.jvm.heap.usage": {
			"value": 0.06603907543771022,
		},
	}
	var jvm driverJvm
	unmarshalJvm(data, &jvm)
	if jvm.GcPsMarkSweepTime != 42 {
		t.Error()
	}
	if jvm.HeapUsage != 0.06603907543771022 {
		t.Error()
	}
}