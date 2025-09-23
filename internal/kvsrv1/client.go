package kvsrv

/*
网络可能会对RPC请求和/或应答进行重排序、延迟或丢弃。为了应对消息丢失的情况，客户端必须不断重试RPC请求，直到收到服务器的应答。
对于带有相同版本号的Put请求，重新发送也是安全的，因为服务器会根据版本号来决定是否执行Put操作；如果服务器已经接收并执行了Put请求，
那么对于重复的Put请求，它会返回rpc.ErrVersion，而不是再次执行Put操作

一个棘手的情况是，如果服务器对客户端重试的RPC请求返回rpc.ErrVersion。在这种情况下，客户端无法确定之前的Put请求是否已经被服务器
执行：之前的RPC请求可能已经被服务器执行，但服务器的应答消息被网络丢弃，导致客户端只收到对重试请求的rpc.ErrVersion应答。
或者，也可能是另一个客户端在之前的RPC请求到达服务器之前更新了键值，导致服务器没有执行任何一个客户端的RPC请求，而是对两个请求都返
回了rpc.ErrVersion。因此，如果客户端收到对重试Put请求的rpc.ErrVersion应答，Clerk.Put函数必须返回rpc.ErrMaybe给应用程序，
而不是rpc.ErrVersion，因为请求可能已经被执行。

如果Put操作能够保证严格的“至多一次”语义（即不会出现rpc.ErrMaybe错误），对于应用程序开发者来说会更加方便。但要实现这一点，需要在
务器端为每个客户端维护状态，这比较困难。
*/

import (
	"time"

	"github.com/luomingguo/TinyKV/internal/kvsrv1/rpc"
	kvtest "github.com/luomingguo/TinyKV/internal/kvtest1"
	tester "github.com/luomingguo/TinyKV/internal/tester1"
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

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := &rpc.GetArgs{
		Key: key,
	}

	for {
		reply := &rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", args, reply)

		if ok {
			// RPC调用成功，检查业务逻辑错误
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	firstTry := true
	for {
		reply := &rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", args, reply)

		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:
				// 操作成功
				return reply.Err

			case rpc.ErrVersion:
				if !firstTry {
					return rpc.ErrMaybe
				}
				// 首次调用返回ErrVersion，说明版本确实不匹配
				return reply.Err
			}
		}
		firstTry = false
		time.Sleep(100 * time.Millisecond)
	}
}
