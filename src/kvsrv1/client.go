package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get 获取某个 key 的当前 value 和 version。
// 如果 key 不存在，返回 ErrNoKey。
// 面对其他所有错误时，它会一直重试下去。
//
// 你可以用类似下面的代码发送 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// args 和 reply 的类型（包括它们是否为指针）
// 必须与 RPC handler 函数声明的参数类型一致。
// 此外，reply 必须以指针形式传入。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{
		Key: key,
	}
	reply := rpc.GetReply{}

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			break
		}
		switch reply.Err {
		case "OK":
			return reply.Value, reply.Version, rpc.OK
		case "ErrNoKey":
			return "", 0, rpc.ErrNoKey
		default:
			return
		}
	}

}

// Put 仅在请求中的 version 与服务端该 key 的 version 匹配时，
// 才用 value 更新 key。如果 version 不匹配，服务端应返回
// ErrVersion。
//
// 如果 Put 在第一次 RPC 时收到 ErrVersion，Put 应返回 ErrVersion，
// 因为这次 Put 肯定没有在服务端执行。
// 如果服务端在一次重发的 RPC 上返回 ErrVersion，
// 那么 Put 必须向应用返回 ErrMaybe，
// 因为更早的那次 RPC 可能已经被服务端成功处理，
// 但响应丢失了，Clerk 无法确定 Put 是否已执行。
//
// 你可以用类似下面的代码发送 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// args 和 reply 的类型（包括它们是否为指针）
// 必须与 RPC handler 函数声明的参数类型一致。
// 此外，reply 必须以指针形式传入。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			break
		}
	}
	switch reply.Err {
	case "OK":
		return rpc.OK
	case "ErrNoKey":
		return rpc.ErrNoKey
	case "ErrVersion":
		return rpc.ErrVersion
	}
}
