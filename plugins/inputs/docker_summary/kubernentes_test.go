package dockersummary

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
)

func TestSummary_getContainerSpecById_multicontainer(t *testing.T) {
	s := mockSummary()
	ass := assert.New(t)

	var pod apiv1.Pod
	buf, err := os.ReadFile("testdata/multicontainer_pod.json")
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(buf, &pod)
	if err != nil {
		panic(err)
	}
	pc, ok := s.getContainerSpecById("docker://9cb11c65234d4779db1eb17581a7891cd9555f6899d4f9c22a54228700da5bbd", &pod)
	ass.True(ok)
	ass.Equal(float64(1), pc.Resources.Requests.Cpu().AsApproximateFloat64())
}