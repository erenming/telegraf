package system

import (
	"reflect"
	"testing"

	"github.com/shirou/gopsutil/disk"
)



func Test_deviceMap1(t *testing.T) {
	type args struct {
		parts []disk.PartitionStat
	}
	tests := []struct {
		name string
		args args
		want map[string]map[string]struct{}
	}{
		{
			name: "",
			args: args{parts: []disk.PartitionStat{
				{
					"/dev/vda1",
					"/rootfs",
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
					"/rootfs": struct {}{},
				},
			},
		},
		{
			name: "",
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
					"/rootfs": struct {}{},
				},
			},
		},
		{
			name: "",
			args: args{parts: []disk.PartitionStat{
				{
					"/dev/vda1",
					"/rootfs",
					"xfs",
					"rw",
				},
				{
					"/dev/vda1",
					"/rootfsxx",
					"xfs",
					"rw",
				},
			}},
			want: map[string]map[string]struct{}{
				"/dev/vda1": {
					"/rootfs": struct {}{},
					"/rootfsxx": struct {}{},
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