package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk 是 k/v clerk 的 Go 接口：
	// 该接口隐藏了 ck 的具体 Clerk 类型，
	// 但保证 ck 支持 Put 和 Get。
	// 测试器在调用 MakeLock() 时会传入这个 clerk。
	ck kvtest.IKVClerk
	// You may add code here
	sockname string //key的值
	id       string //客户端id
}

// 测试器会调用 MakeLock() 并传入一个 k/v clerk；
// 你的代码可以通过调用 lk.ck.Put() 或 lk.ck.Get()
// 来执行 Put 或 Get。
//
// 该接口通过 lockname 参数支持多个锁；
// 不同名字的锁应当是相互独立的。
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	return lk
}

// 不断尝试把空闲 状态改成自己的
func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.sockname)

		if err == rpc.ErrNoKey {
			value = ""
			version = 0
		} else if err == rpc.OK {
			value = lk.id
			return
		}

		if value == "" {
			err := lk.ck.Put(lk.sockname, lk.sockname, version)
			if err == rpc.OK {
				return
			} else if err == rpc.ErrVersion {
				continue
			} else if err == rpc.ErrMaybe {
				lk.ck.Put(lk.sockname, lk.sockname, version)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 完事了释放锁,确定锁是自己的, 条件写是空字符串
func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.sockname)
		if err == rpc.ErrNoKey {
			return
		}
		if err != rpc.OK {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if value == "" || value != lk.id {
			return
		}
		err = lk.ck.Put(lk.sockname, lk.id, version)
		if err == rpc.OK {
			return
		} else if err == rpc.ErrMaybe {
			continue
		} else if err == rpc.ErrVersion {
			time.Sleep(10 * time.Microsecond)
			continue
		}
	}
}
