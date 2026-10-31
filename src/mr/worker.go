package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

// map和reducea交换的数据
type KeyValue struct {
	Key   string
	Value string
}

// map里面处理键的，分成nreduce个桶
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

// 请求任务，wo向co发送,并且接受回复
func requestTask() (TaskReply, bool) {
	var reply TaskReply
	ok := call("Coordinator.GetTask", &TaskArgs{}, &reply)
	return reply, ok
}

// 告诉结果，wo向co发送，不需要回复
func reportTask(task TaskReply, success bool) bool {
	args := TaskReport{
		TaskID:    task.TaskID,
		TaskPhase: task.TaskPhase,
		Attempt:   task.Attempt,
		Success:   success,
	}
	ok := call("Coordinator.ReportTask", &args, &TaskResult{})
	return ok
}

// 执行map函数操作
func runMapTask(task TaskReply, mapf func(string, string) []KeyValue) bool {
	content, err := os.ReadFile(task.TaskName)
	if err != nil {
		return false
	}
	kvs := mapf(task.TaskName, string(content))
	for bucket := 0; bucket < task.NReduce; bucket++ {
		tmp, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			return false
		}
		enc := json.NewEncoder(tmp)
		for _, kv := range kvs {
			if ihash(kv.Key)%task.NReduce == bucket {
				if err := enc.Encode(&kv); err != nil {
					tmp.Close()
					os.Remove(tmp.Name())
					return false
				}
			}
		}
		if err := tmp.Close(); err != nil {
			os.Remove(tmp.Name())
			return false
		}
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskID, bucket)
		if err := os.Rename(tmp.Name(), finalName); err != nil {
			os.Remove(tmp.Name())
			return false
		}
	}
	return true
}
