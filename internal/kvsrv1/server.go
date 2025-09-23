package kvsrv

/*
	构建一个单机KV存储服务器，确保即使在网络故障下，每个Put "至多执行一次"， 并且保证操作的线性一致性。
	我们将这个键值存储服务器来实现一个分布式锁。后续的将基于此服务器实现复制机制，以应对服务器故障。

	每个客户端通过一个客户端代理（Clerk）与键值存储服务器交互，该代理向服务器发送RPC请求。
	客户端可以向服务器发送两种RPC请求：Put(key, value, version) 和 Get(key)。
	版本号记录了该键被写入的次数。Put(key, value, version)只有在Put请求的版本号与服务器中该键的版本号匹配时，
	才会将值更新到映射表中。如果版本号匹配，服务器还会将该键的版本号加1。如果版本号不匹配，
	服务器应返回rpc.ErrVersion。客户端可以通过将版本号设置为0来创建一个新键（服务器存储的版本号将为1）。
	如果Put请求的版本号大于0，但键不存在，服务器应返回rpc.ErrNoKey

*/

import (
	"log"
	"sync"

	"github.com/luomingguo/TinyKV/internal/kvsrv1/rpc"
	"github.com/luomingguo/TinyKV/internal/labrpc"
	tester "github.com/luomingguo/TinyKV/internal/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Val struct {
	Version int64
	Value   string
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	container sync.Map
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	out, ok := kv.container.Load(args.Key)
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	val := out.(Val)
	reply.Err = rpc.OK
	reply.Version = rpc.Tversion(val.Version)
	reply.Value = val.Value
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	reply.Err = rpc.OK // 默认 OK
	cliVerion := int64(args.Version)
	if cliVerion == 0 {
		newVal := Val{
			Version: 1,
			Value:   args.Value,
		}
		// 尝试创建新键，如果键已存在则失败
		_, loaded := kv.container.LoadOrStore(args.Key, newVal)
		if loaded {
			// 键已存在，但客户端版本为0，说明版本不匹配
			reply.Err = rpc.ErrVersion
		}
		return
	}

	currentVal, exists := kv.container.Load(args.Key)
	if !exists {
		// 键不存在但版本号大于0
		reply.Err = rpc.ErrNoKey
		return
	}

	current := currentVal.(Val)

	// 检查版本是否匹配
	if current.Version != cliVerion {
		reply.Err = rpc.ErrVersion
		return
	}

	// 版本匹配，尝试更新
	newVal := Val{
		Version: current.Version + 1,
		Value:   args.Value,
	}

	// 使用 CompareAndSwap 确保原子更新
	if kv.container.CompareAndSwap(args.Key, current, newVal) {
		return
	}
	reply.Err = rpc.ErrVersion

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
