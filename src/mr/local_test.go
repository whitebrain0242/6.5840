package mr

import (
	"testing"
	"time"
)

// 不启动 RPC server，直接构造 Coordinator，方便单元测试
func newTestCoordinator(nMap, nReduce int) *Coordinator {
	c := &Coordinator{nReduce: nReduce}
	for i := 0; i < nMap; i++ {
		c.Map = append(c.Map, Task{
			TaskID:    i,
			TaskName:  "input.txt",
			TaskPhase: MapType,
			State:     Idle,
		})
	}
	for j := 0; j < nReduce; j++ {
		c.Reduce = append(c.Reduce, Task{
			TaskID:    j,
			TaskPhase: ReduceType,
			State:     Idle,
		})
	}
	return c
}

// Map 优先于 Reduce 派发；全部完成后 Done() 为 true
func TestGetTaskOrdersMapBeforeReduce(t *testing.T) {
	c := newTestCoordinator(1, 1)

	// 第一次应拿 Map 0
	var r1 TaskReply
	c.GetTask(&TaskArgs{}, &r1)
	if r1.TaskPhase != MapType || r1.TaskID != 0 {
		t.Fatalf("第一次应拿 Map 0，实际 TaskPhase=%v TaskID=%d", r1.TaskPhase, r1.TaskID)
	}

	// Map 未完成时再拿，应返回 WaitTask
	var r2 TaskReply
	c.GetTask(&TaskArgs{}, &r2)
	if r2.TaskPhase != WaitType {
		t.Fatalf("Map 未完成时应 Wait，实际 TaskPhase=%v", r2.TaskPhase)
	}

	// 报告 Map 完成
	c.ReportTask(&TaskReport{
		TaskID: 0, TaskPhase: MapType, Attempt: r1.Attempt, Success: true,
	}, &TaskResult{})

	// 这时应拿 Reduce 0
	var r3 TaskReply
	c.GetTask(&TaskArgs{}, &r3)
	if r3.TaskPhase != ReduceType || r3.TaskID != 0 {
		t.Fatalf("Map 完成后应拿 Reduce 0，实际 TaskPhase=%v TaskID=%d", r3.TaskPhase, r3.TaskID)
	}

	// 报告 Reduce 完成
	c.ReportTask(&TaskReport{
		TaskID: 0, TaskPhase: ReduceType, Attempt: r3.Attempt, Success: true,
	}, &TaskResult{})

	// Done 应为 true
	if !c.Done() {
		t.Fatalf("全部完成后 Done() 应为 true")
	}
}

// 超时重派：Attempt 增加，旧报告被忽略
func TestTimeoutReassignsAndRejectsStaleReport(t *testing.T) {
	c := newTestCoordinator(1, 1)

	var r1 TaskReply
	c.GetTask(&TaskArgs{}, &r1)
	if r1.TaskID != 0 || r1.TaskPhase != MapType {
		t.Fatalf("第一次应拿 Map 0")
	}
	if r1.Attempt != 1 {
		t.Fatalf("第一次 Attempt 应为 1，实际 %d", r1.Attempt)
	}

	// 把 Starttime 拨回 11 秒前，模拟超时
	c.mu.Lock()
	c.Map[0].Starttime = time.Now().Add(-11 * time.Second)
	c.mu.Unlock()

	// 重派，Attempt 应为 2
	var r2 TaskReply
	c.GetTask(&TaskArgs{}, &r2)
	if r2.TaskID != 0 || r2.Attempt != 2 {
		t.Fatalf("重派应 Map 0 Attempt 2，实际 TaskID=%d Attempt=%d", r2.TaskID, r2.Attempt)
	}

	// 旧 Attempt(1) 报成功，不应生效
	c.ReportTask(&TaskReport{
		TaskID: 0, TaskPhase: MapType, Attempt: 1, Success: true,
	}, &TaskResult{})
	c.mu.Lock()
	s := c.Map[0].State
	c.mu.Unlock()
	if s == Finished {
		t.Fatalf("旧 Attempt 报告不应让任务完成")
	}

	// 新 Attempt(2) 报成功，应完成
	c.ReportTask(&TaskReport{
		TaskID: 0, TaskPhase: MapType, Attempt: 2, Success: true,
	}, &TaskResult{})
	c.mu.Lock()
	s = c.Map[0].State
	c.mu.Unlock()
	if s != Finished {
		t.Fatalf("新 Attempt 报告应让任务完成，实际 state=%v", s)
	}
}

// 失败报告把任务打回 Idle，Done 仍为 false
func TestFailureReportReturnsTaskToPending(t *testing.T) {
	c := newTestCoordinator(1, 1)

	var r1 TaskReply
	c.GetTask(&TaskArgs{}, &r1)

	c.ReportTask(&TaskReport{
		TaskID: 0, TaskPhase: MapType, Attempt: r1.Attempt, Success: false,
	}, &TaskResult{})

	c.mu.Lock()
	s := c.Map[0].State
	c.mu.Unlock()
	if s != Idle {
		t.Fatalf("失败报告后应回到 Idle，实际 state=%v", s)
	}

	if c.Done() {
		t.Fatalf("Map 未完成时 Done() 应为 false")
	}
}
