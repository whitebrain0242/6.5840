package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVvalue struct {
	value   string       //持有者ID
	version rpc.Tversion //每一个key的修改次数
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]KVvalue
}

// u初始化server,还有map
func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]KVvalue)
	return kv
}

// Get 返回 args.Key 对应的 value 和 version，
// 前提是 args.Key 存在。
// 否则，Get 返回 ErrNoKey。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	k := args.Key
	if v, ok := kv.data[k]; ok {
		reply.Value = v.value
		reply.Version = v.version
		reply.Err = "OK"
	} else {
		reply.Err = "ErrNoKey"
	}
}

// 如果 args.Version 与服务端该 key 的 version 匹配，
// 则更新该 key 的 value。
// 如果 version 不匹配，返回 ErrVersion。
// 如果 key 不存在，当 args.Version 为 0 时，
// Put 安装该 value；否则返回 ErrNoKey。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	k := args.Key
	if v, ok := kv.data[k]; !ok {
		if args.Version == 0 {
			v.value = args.Value
			kv.data[k] = v
		} else {
			reply.Err = "ErrNokey"
		}
	} else {
		if args.Version != v.version {
			reply.Err = "ErrVersion"
		} else {
			v.value = args.Value
		}
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
