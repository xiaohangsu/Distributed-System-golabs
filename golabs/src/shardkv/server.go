package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET 		= "Get"
	PUT 		= "Put"
	APPEND 		= "Append"
	RECEIVE		= "Receive"
	RECONFIG 	= "Reconfig"
	CATCHUPLOG 	= "CatchUpLog"
)

type Op struct {
	// Your definitions here.
	OpID		int64
	OpType		string
	Key     	string
	Value   	string
	DestGID		int64
	Num 		int
	RPCIDMap	map[int64]interface{}
	Config		shardmaster.Config
	KVMap           map[string]string // KVMap
	GID 		int64
	Me 		int // Use in Reconfig, only use for run reconfig once.
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	shards	   		[shardmaster.NShards]int64
	config     		shardmaster.Config
	kvmap			map[string]string
	rpcIdMap		map[int64]interface{}
	waitingReceiveGids 	map[int64]bool // Waiting Shards
	waitingConfig		bool
	minIns			int

}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(args.Key)
	if kv.config.Num != kv.sm.Query(-1).Num {
		DPrintf("Get kv.gid=%d kv.me=%d not newest config yet", kv.gid, kv.me)
		reply.Err = ErrNotFinish
		return nil
	}
	if kv.gid != kv.config.Shards[shard] {
		DPrintf("Get Wrong group kv.gid=%d kv.me=%d- config.shards[%d]=%d", kv.gid, kv.me,
			shard,
			kv.config.Shards[shard])
		newConfig := kv.sm.Query(-1)
		DPrintf("Get config.Num %d, newConfig.Num %d", kv.config.Num, newConfig.Num)
		reply.Err = ErrWrongGroup
		return nil
	}
	var value interface{}
	getOp := Op{}
	getOp.OpType 		 = GET
	getOp.OpID   		 = args.Id
	getOp.Key    		 = args.Key
	getOp.GID    		 = kv.gid
	value, reply.Err   = kv.execute(getOp)
	if reply.Err == OK {
		reply.Value = value.(string)
	}
	return nil
}

func (kv *ShardKV) get(op Op) (interface{}, Err) {
	var val interface{}
	var err Err
	var ok bool
	if val, ok = kv.kvmap[op.Key]; ok {
		err = OK
	} else {
		val = nil
		err = ErrNoKey
	}
	kv.rpcIdMap[op.OpID] = val
	return val, err
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("PutAppend kv.gid=%d, kv.me=%d args %v", kv.gid, kv.me,
	args)

	if kv.config.Num != kv.sm.Query(-1).Num {
		DPrintf("Append kv.gid=%d kv.me=%d not newest config yet, kv.config %v, kv.waitingReceiveGids %v, kv.waitingConfig %v", kv.gid, kv.me,
			kv.config, kv.waitingReceiveGids, kv.waitingConfig)
		reply.Err = ErrNotFinish
		return nil
	}
	shard := key2shard(args.Key)

	if kv.gid != kv.config.Shards[shard] {
		DPrintf("PutAppend Wrong group kv.gid=%d - config.shards[%d]=%d, config %v", kv.me, shard, kv.config.Shards[shard],
		kv.config)
		reply.Err = ErrWrongGroup
		return nil
	}
	// Return when saw the Op before
	if _, ok := kv.rpcIdMap[args.Id]; ok {
		reply.Err = OK
		return nil
	}
	putAppendOp := Op{}
	putAppendOp.OpType = args.Op
	putAppendOp.Key = args.Key
	putAppendOp.Value = args.Value
	putAppendOp.OpID = args.Id
	putAppendOp.GID    = kv.gid
	_, reply.Err = kv.execute(putAppendOp)
	return nil
}

func (kv *ShardKV) putAppend(op Op) Err {
	if _, ok := kv.rpcIdMap[op.OpID]; ok {
		return OK
	}
	if op.OpType == PUT {
		DPrintf("kv.gid %d, kv.me %d Put op.Key %v, op.Value %v", kv.gid, kv.me, op.Key, op.Value)
		kv.kvmap[op.Key] = op.Value
	}
	if op.OpType == APPEND {
		kv.kvmap[op.Key] += op.Value
	}
	kv.rpcIdMap[op.OpID] = true
	return OK
}
//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Tick kv.gid %d, kv.me %d, kv.config.Num %d %v Newconfig.Num %d %v", kv.gid, kv.me, kv.config.Num, kv.config.Shards,
	kv.sm.Query(-1).Num, kv.sm.Query(-1).Shards)

	//if kv.waitingConfig {
	//	return
	//}
	kv.catchUpLog()
	config := kv.config
	newConfig := kv.sm.Query(config.Num + 1)

	if config.Num == newConfig.Num {
		return
	}

	if kv.config.Num == 0 {
		kv.config = newConfig
		return
	}

	// Figure out the shard need to send or receive
	send := false
	recv := false
	moveto := make(map[int64]bool)
	shards := make(map[int]int64)
	for i := 0; i < shardmaster.NShards; i++ {
		if newConfig.Shards[i] != config.Shards[i] {
			if config.Shards[i] == kv.gid {
				send = true
				shards[i] = newConfig.Shards[i]
				moveto[newConfig.Shards[i]] = true
			}
			if newConfig.Shards[i] == kv.gid {
				recv = true
				// kv.waitingReceiveGids[config.Shards[i]] = true
			}
		}
	}

	if send && recv {
		DPrintf("ERROR: G%v S%v is both receiving and sending!!!\n", kv.gid, kv.me)
		return
	}

	if send {
		for movetoGID := range moveto {
			reconfigOp := Op{}
			reconfigOp.OpType = RECONFIG
			reconfigOp.Me = kv.me
			reconfigOp.DestGID = movetoGID
			reconfigOp.KVMap = make(map[string]string)
			for key, value := range kv.kvmap {
				shard := key2shard(key)
				if movetoGID == shards[shard] {
					reconfigOp.KVMap[key] = value
					//delete(kv.kvmap, key)
				}
			}
			reconfigOp.RPCIDMap = kv.rpcIdMap
			reconfigOp.OpID = int64(newConfig.Num * 1000000) + movetoGID * 1000 + kv.gid // Send Op ID
			reconfigOp.GID = kv.gid
			reconfigOp.Num = newConfig.Num
			reconfigOp.Config = newConfig

			kv.execute(reconfigOp)
		}
		DPrintf("Reconfig finished kv.gid %d, kv.me %d, config Update kv.config %d, newConfig %d",
			kv.gid, kv.me, kv.config.Num, newConfig.Num)
		kv.config = newConfig
	}

	//if recv {
	//	kv.waitingConfig = true
	//}

	if !recv && !send {
		DPrintf("!recv && !send kv.gid %d, kv.me %d, config Update kv.config %d, newConfig %d",
			kv.gid, kv.me, kv.config.Num, newConfig.Num)
		kv.config = newConfig
	}


}

func (kv *ShardKV) reconfig(op Op) Err {
	if op.Me != kv.me {
		// delete KVMap
		//if !kv.isLeaving() {
		//	for key := range op.KVMap {
		//		delete(kv.kvmap, key)
		//	}
		//}

		return OK
	}

	// Make sure only run one time reconfig
	args := ReceiveArgs{}
	reply := ReceiveReply{}
	args.DestGID = op.DestGID
	args.Num = op.Num
	args.KVmap = op.KVMap
	args.RPCIDmap = op.RPCIDMap
	args.OpId = op.OpID
	args.GID = op.GID

	for {
		to := 10 * time.Millisecond
		DPrintf("Reconfig kv.gid %d, kv.me %d Reconfig %v togid: %d", kv.gid, kv.me, op.Num, op.DestGID)
		for _, server := range op.Config.Groups[op.DestGID] {
			DPrintf("Reconfig kv.gid %d, kv.me %d Server %v", kv.gid, kv.me, server)
			call(server, "ShardKV.Receive", args, &reply) // copy kvserver from current group to future group
			if reply.Err == OK {
				DPrintf("Reconfig kv.gid %d, kv.me %d Finished %v togid: %d", kv.gid, kv.me, op.Num, op.DestGID)
				return OK
			}
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}


}

func (kv *ShardKV) Receive(args *ReceiveArgs, reply *ReceiveReply) error {

	if args.Num <= kv.config.Num {
		DPrintf("Receive kv.gid %d, kv.me %d args.Num %d not matched kv.config.Num %d, args %v OK",
			kv.gid, kv.me, args.Num, kv.config.Num, args)
		reply.Err = OK
		return nil
	} else if args.Num != kv.config.Num + 1 {
		DPrintf("Receive kv.gid %d, kv.me %d args.Num %d not matched kv.config.Num %d, args %v Err",
			kv.gid, kv.me, args.Num, kv.config.Num, args)
		reply.Err = ErrNotFinish
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Num == kv.config.Num + 1 {

		DPrintf("kv.gid %d, kv.me %d, Receive %v, %v %d", kv.gid, kv.me, kv.waitingReceiveGids, kv.waitingConfig, args.DestGID)
		// execute receive
		op := Op{}
		op.OpID = args.OpId
		op.RPCIDMap = args.RPCIDmap
		op.DestGID = args.DestGID
		op.KVMap = args.KVmap
		op.Num = args.Num
		op.GID = args.GID
		op.OpType = RECEIVE
		_, reply.Err = kv.execute(op)
	}
	return nil
}

func (kv *ShardKV) receive(op Op) Err {
	DPrintf("kv.gid %d, kv.me %d - Receive op.Num %d is " +
		"next version kv.config.Num %d. op.OpId %d \n", kv.gid, kv.me,
		op.Num, kv.config.Num, op.OpID)

	if op.Num < kv.config.Num {
		return OK
	}

	if _, ok := kv.rpcIdMap[op.OpID]; ok {
		return OK
	}

	kv.rpcIdMap[op.OpID] = true

	for key, value := range op.KVMap {
		kv.kvmap[key] = value
	}

	// Update with rpcIdMap
	// There might be some old rpc Incoming
	for rpcid, v := range op.RPCIDMap {
		kv.rpcIdMap[rpcid] = v
	}

	delete(kv.waitingReceiveGids, op.GID)
	if len(kv.waitingReceiveGids) == 0 {
		DPrintf("kv.gid %d, kv.me %d receive all Shard data kv.config.Num %d, op.Num %d, %v",
			kv.gid, kv.me, kv.config.Num, op.Num, kv.kvmap)
		kv.config = kv.sm.Query(op.Num)
		kv.waitingConfig = false
	}
	DPrintf("kv.gid %d, kv.me %d kv.waitingReceivedGids %v", kv.gid, kv.me, kv.waitingReceiveGids)
	return OK
}

func (kv *ShardKV) figureOutNeedShards() {
	config := kv.config
	newConfig := kv.sm.Query(config.Num + 1)
	for i := 0; i < shardmaster.NShards; i++ {
		if newConfig.Shards[i] != config.Shards[i] {
			if newConfig.Shards[i] == kv.gid {
				kv.waitingConfig = true
				kv.waitingReceiveGids[config.Shards[i]] = true
			}
		}
	}
}

func (kv *ShardKV) execute(op Op) (v interface{}, err Err) {
	if _, ok := kv.rpcIdMap[op.OpID]; ok {
		return kv.rpcIdMap[op.OpID], OK
	}
	kv.catchUpLog()

	for {
		kv.px.Start(kv.minIns, op)

		to := 10 * time.Millisecond
		for {
			decided, res := kv.px.Status(kv.minIns)
			if decided == paxos.Decided {
				kv.minIns += 1
				op2 := res.(Op)
				if op.OpID != op2.OpID {
					DPrintf("kv.gid %d, kv.me %d kv.minIns %d kv.Num %d - Exectue Op2.OpType %v %v\n",
						kv.gid, kv.me, kv.minIns, kv.config.Num,
						op2.OpType, op2, op)
				} else {
					DPrintf("kv.gid %d, kv.me %d kv.minIns %d kv.Num %d - Exectue Op.OpType %v %v\n",
						kv.gid, kv.me, kv.minIns, kv.config.Num,
						op.OpType, op)
				}
				v, err = kv.applyLog(op2)
				if op.OpID == op2.OpID {
					return v, err
				}
				v, ok := kv.rpcIdMap[op.OpID]
				if ok {
					return v, OK
				}
			} else {
				if to < 1*time.Second {
					to *= 2
				}
				if to >= 1 * time.Second {

					DPrintf("Execute Undecided kv.gid %d, kv.me %d kv.minIns %d op.OpType %v %v Max(): %v",
						kv.gid, kv.me, kv.minIns, op.OpType, op, kv.px.Max())
					return v, ErrWrongGroup
				}
			}

			time.Sleep(to)

		}
	}
	return v, err
}

func (kv *ShardKV) catchUpLog() {
	// Catch up with one step each tick
	to := 10 * time.Millisecond
	for kv.minIns <= kv.px.Max() {
		DPrintf("CatchUpLog kv.gid %d, kv.me %d Apply Log Catch up: kv.minIns %d, Max() %d", kv.gid, kv.me, kv.minIns, kv.px.Max())

		status, v := kv.px.Status(kv.minIns)
		if status == paxos.Decided {
			kv.minIns += 1
			op2 := v.(Op)
			DPrintf("kv.gid %d, kv.me %d kv.minIns %d - Apply Log Catch up: op %v", kv.gid, kv.me, kv.minIns, op2)
			_, err := kv.applyLog(op2)
			DPrintf("kv.gid %d, kv.me %d kv.minIns %d - Apply Log Catch up: op.OpType %v  err %v", kv.gid, kv.me, kv.minIns, op2.OpType, err)

		}
		time.Sleep(to)
		DPrintf("kv.gid %d, kv.me %d Apply Log Catch up: op %v kv.minIns %d status %v", kv.gid, kv.me,
		v, kv.minIns, status)
		if to < 1*time.Second {
			to *= 2
		}
		if to >= 1 * time.Second {
			DPrintf("Still Undecided kv.gid %d, kv.me %d Apply Log Catch up: op %v kv.minIns %d status %v", kv.gid, kv.me,
				v, kv.minIns, status)
			return
		}

	}
}

func (kv *ShardKV) applyLog(op Op) (interface{}, Err) {
	var v interface{}
	var err Err
	if op.OpType == GET {
		v ,err = kv.get(op)
	}
	if op.OpType == PUT || op.OpType == APPEND {
		err = kv.putAppend(op)
	}
	if op.OpType == RECONFIG {
		err = kv.reconfig(op)
	}
	if op.OpType == RECEIVE {
		if !kv.waitingConfig {
			kv.figureOutNeedShards()
		}
		err = kv.receive(op)
	}
	return v, err
}

func (kv *ShardKV) isLeaving() bool {
	nextConfig := kv.sm.Query(kv.config.Num + 1)
	for _, gid := range nextConfig.Shards {
		if gid == kv.gid {
			return false
		}
	}
	return true
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	kv.minIns = 0
	kv.kvmap = make(map[string]string)
	kv.rpcIdMap = make(map[int64]interface{})
	kv.waitingReceiveGids = make(map[int64]bool)
	kv.waitingConfig = false
	kv.config = shardmaster.Config{Num: 0}
	// Don't call Join().

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
