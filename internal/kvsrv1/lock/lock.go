package lock

/*
在许多分布式应用中，运行在不同机器上的客户端会使用键值存储服务来协调各自的操作。例如，ZooKeeper 和 etcd
允许客户端使用分布式锁进行协调，这类似于 Go 程序中线程使用互斥锁（例如 sync.Mutex）进行同步的方式。ZooKeeper
和 etcd 通过条件写入来实现这种分布式锁。
如果持有锁的客户端崩溃，则该锁将永远不会被释放。在更复杂的分布式锁设计中，客户端会为锁设置一个租期。
*/
import (
	"time"

	"github.com/luomingguo/TinyKV/internal/kvsrv1/rpc"
	kvtest "github.com/luomingguo/TinyKV/internal/kvtest1"
)

const (
	LockedVal   string = "0"
	UnlockedVal string = "1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, name: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	//
	for {
		var wantVerion rpc.Tversion
		val, ver, err := lk.ck.Get(lk.name)
		switch err {
		case rpc.OK:
			if val == LockedVal {
				goto redo
			}
			wantVerion = ver
		case rpc.ErrNoKey:
			wantVerion = 0
		default:
			goto redo
		}
		// 返回ErrMaybe
		err = lk.ck.Put(lk.name, LockedVal, wantVerion)
		switch err {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			val2, ver2, err2 := lk.ck.Get(lk.name)
			if err2 == rpc.OK && val2 == LockedVal && ver2 == wantVerion+1 {
				return
			}
		}
	redo:
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.name)
		if err != rpc.OK || val == UnlockedVal {
			return
		}

		err2 := lk.ck.Put(lk.name, UnlockedVal, ver)
		switch err2 {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			val2, _, gerr := lk.ck.Get(lk.name)
			if gerr == rpc.OK && val2 == UnlockedVal {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
