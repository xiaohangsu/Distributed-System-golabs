package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	minIns     int
	configs    []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpID 		int64
	OpType    	string
	GID       	int64
	Servers   	[]string
	ShardNum	int
	ConfigNum	int
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	// Did not check if same as previous or not
	sm.mu.Lock()
	defer sm.mu.Unlock()
	joinOp := Op{}
	joinOp.OpID = nrand()
	joinOp.OpType = JOIN
	joinOp.GID = args.GID
	joinOp.Servers = args.Servers
	sm.execute(joinOp, nil)
	return nil
}

func (sm *ShardMaster) join(op Op) error {
	if op.GID < 0 {
		return nil
	}

	prevConfig := sm.configs[len(sm.configs) - 1]
	newConfig := Config{Num: prevConfig.Num + 1}
	newConfig.Groups = make(map[int64][]string)

	// Update Groups
	for k, v := range prevConfig.Groups {
		newConfig.Groups[k] = v
	}
	newConfig.Groups[op.GID] = op.Servers

	newConfig.Shards = sm.reassignShards(newConfig)
	sm.configs = append(sm.configs, newConfig)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	leaveOp := Op{}
	leaveOp.OpID = nrand()
	leaveOp.OpType = LEAVE
	leaveOp.GID = args.GID
	sm.execute(leaveOp, nil)
	return nil
}

func (sm *ShardMaster) leave(op Op) error {
	if op.GID < 0 {
		return nil
	}

	prevConfig := sm.configs[len(sm.configs) - 1]
	// return if not in Groups
	_, ok := prevConfig.Groups[op.GID]
	if !ok {
		return nil
	}

	newConfig := Config{Num: prevConfig.Num + 1}
	newConfig.Groups = make(map[int64][]string)

	// Update Groups
	for k, v := range prevConfig.Groups {
		newConfig.Groups[k] = v
	}

	delete(newConfig.Groups, op.GID)

	newConfig.Shards = sm.reassignShards(newConfig)
	sm.configs = append(sm.configs, newConfig)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	moveOp := Op{}
	moveOp.OpID = nrand()
	moveOp.OpType = MOVE
	moveOp.GID = args.GID
	moveOp.ShardNum = args.Shard
	sm.execute(moveOp, nil)
	return nil
}

func (sm *ShardMaster) move(op Op) error {
	if op.GID < 0 {
		return nil
	}

	prevConfig := sm.configs[len(sm.configs) - 1]

	// If args.GID is not in Groups
	_, ok := prevConfig.Groups[op.GID]; if !ok {
		return nil
	}

	// If Shard already in GID
	if prevConfig.Shards[op.ShardNum] == op.GID {
		return nil
	}

	newConfig := Config{Num: prevConfig.Num + 1}
	newConfig.Groups = make(map[int64][]string)

	// Update Groups
	for k, v := range prevConfig.Groups {
		newConfig.Groups[k] = v
	}

	prevConfig.Shards[op.ShardNum] = op.GID
	newConfig.Shards = prevConfig.Shards

	sm.configs = append(sm.configs, newConfig)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	queryOp := Op{}
	queryOp.OpID = nrand()
	queryOp.OpType = QUERY
	queryOp.ConfigNum = args.Num
	sm.execute(queryOp, reply)
	return nil
}

func (sm *ShardMaster) query(op Op) Config {
	prevConfig := sm.configs[len(sm.configs) - 1]
	if op.ConfigNum < 0 || op.ConfigNum >= len(sm.configs) {
		return prevConfig
	} else {
		return sm.configs[op.ConfigNum]
	}
}

func (sm *ShardMaster) execute(op Op, reply *QueryReply) error {
	for {
		sm.px.Start(sm.minIns, op)

		to := 10 * time.Millisecond
		for {
			decided, res := sm.px.Status(sm.minIns)
			if decided == paxos.Decided {
				sm.minIns += 1
				receiveOp := res.(Op)
				if receiveOp.OpType == JOIN {
					sm.join(receiveOp)
				} else if receiveOp.OpType == LEAVE {
					sm.leave(receiveOp)
				} else if receiveOp.OpType == MOVE {
					sm.move(receiveOp)
				}
				//  Can skip Query

				sm.px.Done(sm.minIns - 1)
				if op.OpID == receiveOp.OpID {
					if op.OpType != receiveOp.OpType {
						fmt.Printf("ERROR: Different ops with the same uid!!!\n")
					}
					if op.OpType == QUERY {
						reply.Config = sm.query(op)
					}
					return nil
				}
				break
			}

			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}
	}
}

func (sm *ShardMaster) reassignShards(newConfig Config) [NShards]int64 {
	oldConfig := sm.configs[len(sm.configs) - 1]
	newGidsLen := len(newConfig.Groups)

	// Create old gids
	var oldGids []int64
	for k := range oldConfig.Groups {
		oldGids = append(oldGids, k)
	}

	// create new gids
	var newGids []int64
	for k := range newConfig.Groups {
		newGids = append(newGids, k)
	}

	shards := oldConfig.Shards

	// create old count map
	oldGidsMap := make(map[int64]int)
	for k := 0; k < NShards; k++ {
		oldGidsMap[shards[k]]++
	}

	size := 1
	if (newGidsLen < NShards) {
		size = NShards / newGidsLen
	}


	i := 0
	for j:= 0; j < NShards; j++ {
		_, ok := newConfig.Groups[shards[j]]
		if oldGidsMap[shards[j]] > size || shards[j] == 0 || !ok {

			oldGidsMap[shards[j]]--
			temp := i
			for oldGidsMap[newGids[i]] >= size {
				i++
				i = i % newGidsLen

				if temp == i {
					break
				}
			}
			shards[j] = newGids[i]
			i++
			i = i % newGidsLen
		}
	}

	return shards
}


// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
