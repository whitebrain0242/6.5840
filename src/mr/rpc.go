package mr

type TaskType int

const (
	MapType TaskType = iota
	ReduceType
	WaitType
	DoneType
)

// 这个是workern向con申请任务
type TaskArgs struct {
}

// 这个是co向worker分配任务
type TaskReply struct {
	TaskID    int
	TaskName  string
	TaskPhase TaskType
	NReduce   int
	Attempt   int //分配次数
}

// 这各是worker完成任务的时候向co发送
type TaskReport struct {
	TaskID    int
	TaskPhase TaskType
	Attempt   int
	Success   bool
}

// 这个是co受到wo发送的完成任务的i回复
type TaskResult struct{}
