package kubernetes

const (
	DeleteEvent EventState = "delete"
	AddEvent    EventState = "add"
)

type EventState string

type Item struct {
	State EventState
	Key   interface{}
	Obj   interface{}
}
