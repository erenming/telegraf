package spark

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type jobModel struct {
	JobId              int        `json:"jobId"`
	Name               string     `json:"name"`
	SubmissionTime     *sparkTime `json:"submissionTime"`
	CompletionTime     *sparkTime `json:"completionTime"`
	StageIds           []int      `json:"stageIds"`
	Status             string     `json:"status"`
	NumTasks           int        `metric:"num_tasks" json:"numTasks"`
	NumActiveTasks     int        `metric:"num_active_tasks" json:"numActiveTasks"`
	NumCompletedTasks  int        `metric:"num_completed_tasks" json:"numCompletedTasks"`
	NumSkippedTasks    int        `metric:"num_skipped_tasks" json:"numSkippedTasks"`
	NumFailedTasks     int        `metric:"num_failed_tasks" json:"numFailedTasks"`
	NumKilledTasks     int        `metric:"num_killed_tasks" json:"numKilledTasks"`
	NumActiveStages    int        `metric:"num_active_stages" json:"numActiveStages"`
	NumCompletedStages int        `metric:"num_completed_stages" json:"numCompletedStages"`
	NumSkippedStages   int        `metric:"num_skipped_stages" json:"numSkippedStages"`
	NumFailedStages    int        `metric:"num_failed_stages" json:"numFailedStages"`
}

type stageModel struct {
	StageId             int              `json:"stageId"`
	Name                string           `json:"name"`
	Status              string           `json:"status"`
	NumTasks            int              `metric:"num_tasks" json:"totalTasks"`
	NumActiveTasks      int              `metric:"num_active_tasks" json:"activeTasks"`
	NumCompletedTasks   int              `metric:"num_completed_tasks" json:"completedTasks"`
	NumFailedTasks      int              `metric:"num_failed_tasks" json:"failedTasks"`
	NumKilledTasks      int              `metric:"num_killed_tasks" json:"killedTasks"`
	ExecutorRunTime     int              `metric:"executor_run_time" json:"executorRunTime"`
	SubmissionTime      *sparkTime       `json:"submissionTime"`
	CompletionTime      *sparkTime       `json:"completionTime"`
	FailureReason       *json.RawMessage `metric:"failure_reason" json:"failureReason"`
	InputBytes          int              `metric:"input_bytes" json:"inputBytes"`
	InputRecords        int              `metric:"input_records" json:"inputRecords"`
	OutputBytes         int              `metric:"output_bytes" json:"outputBytes"`
	OutputRecords       int              `metric:"output_records" json:"outputRecords"`
	ShuffleReadBytes    int              `metric:"shuffle_read_bytes" json:"shuffleReadBytes"`
	ShuffleReadRecords  int              `metric:"shuffle_read_records" json:"shuffleReadRecords"`
	ShuffleWriteBytes   int              `metric:"shuffle_write_bytes" json:"shuffleWriteBytes"`
	ShuffleWriteRecords int              `metric:"shuffle_write_records" json:"shuffleWriteRecords"`
	MemoryBytesSpilled  int              `metric:"memory_bytes_spilled" json:"memoryBytesSpilled"`
	DiskBytesSpilled    int              `metric:"disk_bytes_spilled" json:"diskBytesSpilled"`
}

type executorModel struct {
	Id                string `json:"id"`
	HostPort          string `json:"hostPort"`
	DiskUsed          int    `metric:"disk_used" json:"diskUsed"`
	NumTasks          int    `metric:"num_tasks" json:"totalTasks"`
	NumActiveTasks    int    `metric:"num_active_tasks" json:"activeTasks"`
	NumCompletedTasks int    `metric:"num_completed_tasks" json:"completedTasks"`
	NumFailedTasks    int    `metric:"num_failed_tasks" json:"failedTasks"`
	RddBlocks         int    `metric:"rdd_blocks" json:"rddBlocks"`
	MemoryUsed        int    `metric:"memory_used" json:"memoryUsed"`
	MaxMemory         int    `metric:"max_memory" json:"maxMemory"`
	TotalDuration     int    `metric:"total_duration" json:"totalDuration"`
	TotalGCTime       int    `metric:"total_gc_time" json:"totalGCTime"`
	TotalInputBytes   int    `metric:"total_input_bytes" json:"totalInputBytes"`
	TotalShuffleRead  int    `metric:"total_shuffle_read" json:"totalShuffleRead"`
	TotalShuffleWrite int    `metric:"total_shuffle_write" json:"totalShuffleWrite"`
}

type driverJvm struct {
	HeapInit           int     `metric:"heap_init" suffix:"driver.jvm.heap.init"`
	HeapMax            int     `metric:"heap_max" suffix:"driver.jvm.heap.max"`
	HeapUsage          float64 `metric:"heap_usage" suffix:"driver.jvm.heap.usage"`
	HeapUsed           int     `metric:"heap_used" suffix:"driver.jvm.heap.used"`
	NonHeapInit        int     `metric:"non_heap_init" suffix:"driver.jvm.non-heap.init"`
	NonHeapMax         int     `metric:"non_heap_max" suffix:"driver.jvm.non-heap.max"`
	NonHeapUsage       float64 `metric:"non_heap_usage" suffix:"driver.jvm.non-heap.usage"`
	NonHeapUsed        int     `metric:"non_heap_used" suffix:"driver.jvm.non-heap.used"`
	DirectCapacity     int     `metric:"direct_capacity" suffix:"driver.jvm.direct.capacity"`
	DirectUsed         int     `metric:"direct_used" suffix:"driver.jvm.direct.used"`
	MappedCapacity     int     `metric:"mapped_capacity" suffix:"driver.jvm.mapped.capacity"`
	MappedCount        int     `metric:"mapped_count" suffix:"driver.jvm.mapped.count"`
	MappedUsed         int     `metric:"mapped_used" suffix:"driver.jvm.mapped.used"`
	GcPsMarkSweepCount int     `metric:"gc_ps_mark_sweep_count" suffix:"driver.jvm.PS-MarkSweep.count"`
	GcPsScavengeCount  int     `metric:"gc_ps_scavenge_count" suffix:"driver.jvm.PS-Scavenge.count"`
	GcPsMarkSweepTime  int     `metric:"gc_ps_mark_sweep_time" suffix:"driver.jvm.PS-MarkSweep.time"`
	GcPsScavengeTime   int     `metric:"gc_ps_scavenge_time" suffix:"driver.jvm.PS-Scavenge.time"`
}

type sparkTime struct {
	*time.Time
}

func (st *sparkTime) UnmarshalJSON(input []byte) error {
	strInput := string(input)
	strInput = strings.Trim(strInput, `"`)
	if strInput == "" {
		st.Time = nil
		return nil
	}
	newTime, err := time.Parse("2006-01-02T15:04:05.000GMT", strInput)
	if err != nil {
		return err
	}
	st.Time = &newTime
	return nil
}

func populateMetric(fields map[string]interface{}, job interface{}) error {
	if reflect.TypeOf(job).Kind() != reflect.Ptr {
		return fmt.Errorf("param job must be ptr")
	}
	if reflect.TypeOf(job).Elem().Kind() != reflect.Struct {
		return fmt.Errorf("job entity must be struct")
	}
	et, ev := reflect.TypeOf(job).Elem(), reflect.ValueOf(job).Elem()
	for i := 0; i < et.NumField(); i++ {
		mtag, ok := et.Field(i).Tag.Lookup("metric")
		if !ok {
			continue
		}
		fields[mtag] = ev.Field(i).Interface()
	}
	return nil
}

func unmarshalJvm(data map[string]map[string]interface{}, jvm *driverJvm) error {
	var prefix string
	for k := range data {
		if rv := strings.SplitN(k, ".", 2); len(rv) != 2 {
			return fmt.Errorf("bad data")
		} else {
			prefix = rv[0]
		}
		break
	}

	et, ev := reflect.TypeOf(jvm).Elem(), reflect.ValueOf(jvm).Elem()
	for i := 0; i < et.NumField(); i++ {
		tag, ok := et.Field(i).Tag.Lookup("suffix")
		if !ok {
			continue
		}
		val, ok := data[strings.Join([]string{prefix, tag}, ".")]
		if !ok {
			continue
		}
		switch et.Field(i).Type.Kind() {
		case reflect.Int, reflect.Int64:
			ev.Field(i).SetInt(int64(val["value"].(float64)))
		case reflect.Float64:
			ev.Field(i).SetFloat(val["value"].(float64))
		default:
			return fmt.Errorf("no support field %s:%s", et.Field(i).Name, et.Field(i).Type.Name())
		}
	}
	return nil
}
