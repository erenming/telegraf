package dcos_app_status

type AppStatus struct {
	Id             string `json:"id,omitempty"`
	Instances      int32  `json:"instances,omitempty"`
	TasksHealthy   int32  `json:"tasksHealthy,omitempty"`
	TasksRunning   int32  `json:"tasksRunning,omitempty"`
	TasksStaged    int32  `json:"tasksStaged,omitempty"`
	TasksUnhealthy int32  `json:"tasksUnhealthy,omitempty"`
}

type DCOSApp struct {
	App *AppStatus `json:"app,omitempty"`
}

type DCOSGroup struct {
	Group []*AppStatus `json:"*,omitempty"`
}
