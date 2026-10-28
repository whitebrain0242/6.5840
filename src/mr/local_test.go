package mr

import (
	"testing"
	"time"
)

// 辅助：造一个干净的 Coordinator
func newTestCoordinator(nMap, nReduce int) *Coordinator {
	c := &Coordinator{
		nReduce: nReduce,
	}
	for i := 0; i < nMap; i++ {
		c.mapTasks = append(c.mapTasks, TaskInfo{
			Type:  MapTask,
			Index: i,
			Input: "input.txt",
			State: TaskPending,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, TaskInfo{
			Type:  ReduceTask,
			Index: i,
			State: TaskPending,
		})
	}
	return c
}

func TestGetTaskOrdersMapBeforeReduce(t *testing.T) {
	c := newTestCoordinator(1, 1)

	// 第一次应拿 Map
	var r1 GetTaskReply
	c.GetTask(&GetTaskArgs{}, &r1)
	if r1.Type != MapTask || r1.Index != 0 {
		t.Fatalf("第一次应拿 Map 0，实际 Type=%v Index=%d", r1.Type, r1.Index)
	}

	// Map 未完成时再拿，应返回 WaitTask
	var r2 GetTaskReply
	c.GetTask(&GetTaskArgs{}, &r2)
	if r2.Type != WaitTask {
		t.Fatalf("Map 未完成时应 Wait，实际 Type=%v", r2.Type)
	}

	// 报告 Map 完成
	c.ReportTask(&ReportTaskArgs{
		Type: MapTask, Index: 0, Attempt: r1.Attempt, Success: true,
	}, &ReportTaskReply{})

	// 这时应拿 Reduce
	var r3 GetTaskReply
	c.GetTask(&GetTaskArgs{}, &r3)
	if r3.Type != ReduceTask || r3.Index != 0 {
		t.Fatalf("Map 完成后应拿 Reduce 0，实际 Type=%v Index=%d", r3.Type, r3.Index)
	}

	// 报告 Reduce 完成
	c.ReportTask(&ReportTaskArgs{
		Type: ReduceTask, Index: 0, Attempt: r3.Attempt, Success: true,
	}, &ReportTaskReply{})

	// Done 应为 true
	if !c.Done() {
		t.Fatalf("全部完成后 Done() 应为 true")
	}
}

func TestTimeoutReassignsAndRejectsStaleReport(t *testing.T) {
	c := newTestCoordinator(1, 1)

	// 第一次拿 Map 0，Attempt=1
	var r1 GetTaskReply
	c.GetTask(&GetTaskArgs{}, &r1)
	if r1.Attempt != 1 {
		t.Fatalf("第一次 Attempt 应为 1，实际 %d", r1.Attempt)
	}

	// 手动把分配时间拨回 11 秒前
	c.mu.Lock()
	c.mapTasks[0].AssignedAt = time.Now().Add(-11 * time.Second)
	c.mu.Unlock()

	// 再拿，应重派 Map 0，Attempt=2
	var r2 GetTaskReply
	c.GetTask(&GetTaskArgs{}, &r2)
	if r2.Index != 0 || r2.Attempt != 2 {
		t.Fatalf("重派应 Map 0 Attempt 2，实际 Index=%d Attempt=%d", r2.Index, r2.Attempt)
	}

	// 旧 Attempt 报告成功，不应生效
	c.ReportTask(&ReportTaskArgs{
		Type: MapTask, Index: 0, Attempt: 1, Success: true,
	}, &ReportTaskReply{})
	c.mu.Lock()
	s := c.mapTasks[0].State
	c.mu.Unlock()
	if s == TaskFinished {
		t.Fatalf("旧 attempt 不应让任务完成")
	}

	// 新 Attempt 报告成功，应完成
	c.ReportTask(&ReportTaskArgs{
		Type: MapTask, Index: 0, Attempt: 2, Success: true,
	}, &ReportTaskReply{})
	c.mu.Lock()
	s = c.mapTasks[0].State
	c.mu.Unlock()
	if s != TaskFinished {
		t.Fatalf("新 attempt 应让任务完成，实际 state=%v", s)
	}
}

func TestFailureReportReturnsTaskToPending(t *testing.T) {
	c := newTestCoordinator(1, 1)

	var r1 GetTaskReply
	c.GetTask(&GetTaskArgs{}, &r1)

	// 用失败报告
	c.ReportTask(&ReportTaskArgs{
		Type: MapTask, Index: 0, Attempt: r1.Attempt, Success: false,
	}, &ReportTaskReply{})

	// 任务应回到 Pending
	c.mu.Lock()
	s := c.mapTasks[0].State
	c.mu.Unlock()
	if s != TaskPending {
		t.Fatalf("失败报告后应回到 Pending，实际 state=%v", s)
	}

	// Done 仍为 false
	if c.Done() {
		t.Fatalf("Map 未完成时 Done() 应为 false")
	}
}
