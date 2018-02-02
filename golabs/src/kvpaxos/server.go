package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import (
	"math/rand"
	"time"
)


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	Key string
	Value string
	Id int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	// Your definitions here.
	kvMap      map[string]string
	opIdMap	   map[int64]bool
	minSeq     int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Key: args.Key, Op: GET, Id: args.Id}

	kv.retryTillOK(op)

	if v, ok := kv.kvMap[args.Key]; !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = v
	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.opIdMap[args.Id] {
		reply.Err = OK
		return nil
	}

	op := Op{}
	op.Value = args.Value
	op.Key = args.Key
	op.Op = args.Op
	op.Id = args.Id

	kv.retryTillOK(op)
	reply.Err = OK

	return nil
}

// Return final fix Seq
func (kv *KVPaxos) retryTillOK(op Op) {
	var err Err
	var log Op
	for err != OK {
		seq := kv.minSeq
		status, val := kv.px.Status(kv.minSeq)
		fmt.Println(op)

		if status == paxos.Decided {
			log = val.(Op)
		} else {
			kv.px.Start(seq, op)
			log = kv.wait(seq)
		}

		if log.Id == op.Id {
			err = OK
		}
		kv.applyLog(log)
	}
}

func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			kvlog, _ := v.(Op)
			return kvlog
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv * KVPaxos) applyLog(op Op) {
	if op.Op == APPEND {
		kv.kvMap[op.Key] += op.Value
	}
	if op.Op == PUT {
		if _, ok := kv.kvMap[op.Key]; !ok {
			kv.kvMap[op.Key] = ""
		}
		kv.kvMap[op.Key] = op.Value
	}
	kv.opIdMap[op.Id] = true
	kv.px.Done(kv.minSeq)
	kv.minSeq += 1
	// we ignore Get
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.

	kv.kvMap = make(map[string]string)
	kv.minSeq = 1
	kv.opIdMap = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
