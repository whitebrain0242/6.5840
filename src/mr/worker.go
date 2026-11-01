package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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

func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		reply, ok := requestTask()
		if !ok {
			return
		}

		if reply.TaskPhase == MapType {
			success := runMapTask(reply, mapf)
			if ok := reportTask(reply, success); !ok {
				return
			}
		} else if reply.TaskPhase == ReduceType {
			success := runReduceTask(reply, reducef)
			if ok := reportTask(reply, success); !ok {
				return
			}
		} else if reply.TaskPhase == WaitType {
			time.Sleep(100 * time.Millisecond)
		} else if reply.TaskPhase == DoneType {
			return
		}

	}
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

// 不需要遍历桶：一个桶输出一个输出文件
func runReduceTask(task TaskReply, reducef func(string, []string) string) bool {
	//1,读文件,进行解码
	var kvs []KeyValue
	//遍历每一个文件
	for i := 0; i < task.NMap; i++ {
		fname := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		f, err := os.Open(fname)
		if err != nil {
			return false
		}
		dec := json.NewDecoder(f)
		//绑定好解码器,p之后开始解码
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				f.Close()
				return false
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}
	//2.对文件进行排序,按照i名字
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	//3.创建一个临时文件,然后分组之后调用reducef,写进去
	tmp, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		return false
	}
	for i := 0; i < len(kvs); {
		key := kvs[i].Key
		var values []string
		for i < len(kvs) && kvs[i].Key == key {
			values = append(values, kvs[i].Value)
			i++
		}
		output := reducef(key, values)
		fmt.Fprintf(tmp, "%v %v\n", key, output)
	}
	//4. close + rename
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return false
	}
	finalName := fmt.Sprintf("mr-out-%d", task.TaskID)
	if err := os.Rename(tmp.Name(), finalName); err != nil {
		os.Remove(tmp.Name())
		return false
	}
	return true
}
