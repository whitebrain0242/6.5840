package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// 不启动 RPC server，直接构造 Coordinator，供单元测试使用
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

// ===== Task 2 测试 =====

func TestGetTaskOrdersMapBeforeReduce(t *testing.T) {
	c := newTestCoordinator(1, 1)

	var r1 TaskReply
	c.GetTask(&TaskArgs{}, &r1)
	if r1.TaskPhase != MapType || r1.TaskID != 0 {
		t.Fatalf("第一次应拿 Map 0，实际 TaskPhase=%v TaskID=%d", r1.TaskPhase, r1.TaskID)
	}

	var r2 TaskReply
	c.GetTask(&TaskArgs{}, &r2)
	if r2.TaskPhase != WaitType {
		t.Fatalf("Map 未完成时应 Wait，实际 TaskPhase=%v", r2.TaskPhase)
	}

	c.ReportTask(&TaskReport{
		TaskID: 0, TaskPhase: MapType, Attempt: r1.Attempt, Success: true,
	}, &TaskResult{})

	var r3 TaskReply
	c.GetTask(&TaskArgs{}, &r3)
	if r3.TaskPhase != ReduceType || r3.TaskID != 0 {
		t.Fatalf("Map 完成后应拿 Reduce 0，实际 TaskPhase=%v TaskID=%d", r3.TaskPhase, r3.TaskID)
	}

	c.ReportTask(&TaskReport{
		TaskID: 0, TaskPhase: ReduceType, Attempt: r3.Attempt, Success: true,
	}, &TaskResult{})

	if !c.Done() {
		t.Fatalf("全部完成后 Done() 应为 true")
	}
}

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

	c.mu.Lock()
	c.Map[0].Starttime = time.Now().Add(-11 * time.Second)
	c.mu.Unlock()

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

// ===== Task 3 测试 =====

func TestMapTaskWritesNReduceJSONBuckets(t *testing.T) {
	dir := t.TempDir()
	oldwd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldwd)

	if err := os.WriteFile("input.txt", []byte("a b c"), 0644); err != nil {
		t.Fatal(err)
	}

	mapf := func(filename, content string) []KeyValue {
		return []KeyValue{{"a", "1"}, {"b", "1"}, {"c", "1"}}
	}

	task := TaskReply{
		TaskPhase: MapType,
		TaskID:    0,
		TaskName:  "input.txt",
		NReduce:   3,
	}
	if !runMapTask(task, mapf) {
		t.Fatalf("runMapTask 应返回 true")
	}

	// 每个桶里的键必须满足 ihash(key)%3 == 桶号
	for bucket := 0; bucket < 3; bucket++ {
		fname := fmt.Sprintf("mr-0-%d", bucket)
		f, err := os.Open(fname)
		if err != nil {
			t.Fatalf("缺少文件 %s", fname)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				t.Fatalf("解码 %s 失败: %v", fname, err)
			}
			if ihash(kv.Key)%3 != bucket {
				t.Fatalf("键 %s 应在桶 %d，实际在 %d", kv.Key, ihash(kv.Key)%3, bucket)
			}
		}
		f.Close()
	}

	// 不能遗留临时文件
	entries, _ := os.ReadDir(".")
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), "mr-tmp") || strings.Contains(e.Name(), "mr-tmp") {
			t.Fatalf("遗留临时文件 %s", e.Name())
		}
	}
}
