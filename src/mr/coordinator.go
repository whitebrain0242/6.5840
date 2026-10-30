package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 任务的状态
type TaskState int

const (
	Idle TaskState = iota
	Inprogress
	Finished
)

// 任务定义
type Task struct {
	TaskID    int
	TaskName  string
	TaskPhase TaskType
	State     TaskState
	Attempt   int
	Starttime time.Time
}

type Coordinator struct {
	mu      sync.Mutex
	Map     []Task
	Reduce  []Task
	nReduce int
}

// RPC接口开始监听
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// 询问有没有完成
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// 建立任务表
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 创建Map任务
	for i, f := range files {
		c.Map = append(c.Map, Task{
			TaskID:    i,
			TaskName:  f,
			TaskPhase: MapType,
			State:     Idle,
		})
	}
	//创建reduce任务
	c.nReduce = nReduce
	for j := 0; j < nReduce; j++ {
		c.Reduce = append(c.Reduce, Task{
			TaskID:    j,
			TaskPhase: ReduceType,
			State:     Idle,
			TaskName:  "",
		})
	}

	c.server(sockname)
	return &c
}

// ===
func (c *Coordinator) checkTimeout(tasks []Task) {
	for i := range tasks {
		if tasks[i].State == Inprogress && time.Since(tasks[i].Starttime) > 10*time.Second { //超时
			tasks[i].State = Idle
		}
	}
}
func (c *Coordinator) selectTask(tasks []Task) int {
	for i := range tasks {
		if tasks[i].State == Idle {
			return i
		}
	}
	return -1
}
func (c *Coordinator) mapDone() bool {
	for _, task := range c.Map {
		if task.State != Finished {
			return false
		}
	}
	return true
}
func (c *Coordinator) reduceDone() bool {
	for _, task := range c.Reduce {
		if task.State != Finished {
			return false
		}
	}
	return true
}

// 发任务
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock() //自动管理生命周期
	//检查超时
	c.checkTimeout(c.Map)
	c.checkTimeout(c.Reduce)

	if !c.mapDone() {
		//map阶段
		if index := c.selectTask(c.Map); index >= 0 {
			//map改完
			c.Map[index].State = Inprogress
			c.Map[index].Attempt++
			c.Map[index].Starttime = time.Now()
			//改reply
			reply.TaskID = index
			reply.TaskName = c.Map[index].TaskName
			reply.TaskPhase = MapType
			reply.NReduce = c.nReduce
			reply.Attempt = c.Map[index].Attempt
			reply.NMap = len(c.Map)

		} else {
			reply.TaskPhase = WaitType
		}
		return nil
	}
	if !c.reduceDone() {
		if i := c.selectTask(c.Reduce); i >= 0 {
			//reduce阶段
			c.Reduce[i].State = Inprogress
			c.Reduce[i].Attempt++
			c.Reduce[i].Starttime = time.Now()
			//改reply
			reply.TaskID = i
			reply.TaskPhase = ReduceType
			reply.Attempt = c.Reduce[i].Attempt
			reply.NMap = len(c.Map) //决定读几个中间文件
			reply.NReduce = c.nReduce
		} else {
			reply.TaskPhase = WaitType
		}
		return nil
	}
	reply.TaskPhase = DoneType
	return nil
}

// 核对后标记完成
func (c *Coordinator) ReportTask(report *TaskReport, result *TaskResult) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if report.TaskPhase == MapType {
		//map任务
		if report.TaskID < 0 || report.TaskID > len(c.Map)-1 {
			return nil
		}
		if c.Map[report.TaskID].Attempt != report.Attempt {
			return nil
		}
		if c.Map[report.TaskID].State == Finished {
			return nil
		}
		if report.Success && c.Map[report.TaskID].State == Inprogress {
			c.Map[report.TaskID].State = Finished
		}
		if !report.Success {
			c.Map[report.TaskID].State = Idle
		}

	} else if report.TaskPhase == ReduceType {
		//reduce任务
		if report.TaskID < 0 || report.TaskID > len(c.Reduce)-1 {
			return nil
		}
		if c.Reduce[report.TaskID].Attempt != report.Attempt {
			return nil
		}
		if c.Reduce[report.TaskID].State == Finished {
			return nil
		}
		//要先判断是不是当前worker手里的，如果不是并且s失败了，那么结果就不能作为参考
		if report.Success && c.Reduce[report.TaskID].State == Inprogress {
			c.Reduce[report.TaskID].State = Finished
		}
		if !report.Success {
			c.Reduce[report.TaskID].State = Idle
		}

	}
	return nil
}
