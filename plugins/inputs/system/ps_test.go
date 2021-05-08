package system

import (
	"reflect"
	"testing"

	"github.com/shirou/gopsutil/disk"
)


func Test_deviceMap(t *testing.T) {


	type args struct {
		parts []disk.PartitionStat
	}
	tests := []struct {
		name string
		args args
		want map[string]map[string]struct{}
	}{
		{
			name: "same device with one path",
			args: args{parts: []disk.PartitionStat{
				{
					"/dev/vda1",
					"/etc/hostname",
					"xfs",
					"rw",
				},
				{
					"/dev/vda1",
					"/data",
					"xfs",
					"rw",
				},
				{
					"/dev/vda1",
					"/etc/hosts",
					"xfs",
					"rw",
				},
				{
					"/dev/vda1",
					"/data/docker/xxx/yyy",
					"xfs",
					"rw",
				},
				{
					"/dev/vda1",
					"/data/tmp",
					"xfs",
					"rw",
				},
			}},
			want: map[string]map[string]struct{}{
				"/dev/vda1": {
					"/data":         {},
					"/etc/hosts":    {},
					"/etc/hostname": {},
				},
			},
		},
		{
			name: "multi mount with same device",
			args: args{parts: []disk.PartitionStat{
				{
					"/dev/vda1",
					"/data",
					"xfs",
					"rw",
				},
				{
					"/dev/vda1",
					"/data/xxx",
					"xfs",
					"rw",
				},
				{
					"/dev/vda1",
					"/datax",
					"xfs",
					"rw",
				},
			}},
			want: map[string]map[string]struct{}{
				"/dev/vda1": {
					"/data":  struct{}{},
					"/datax": struct{}{},
				},
			},
		},
		{
			name: "rootfs",
			args: args{parts: []disk.PartitionStat{
				{
					"/dev/vda1",
					"/rootfs/tmp",
					"xfs",
					"rw",
				},
				{
					"/dev/vda1",
					"/rootfs",
					"xfs",
					"rw",
				},
			}},
			want: map[string]map[string]struct{}{
				"/dev/vda1": {
					"/rootfs": struct{}{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deviceMap(tt.args.parts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deviceMap() = %v, want %v", got, tt.want)
			}
		})
	}
}
