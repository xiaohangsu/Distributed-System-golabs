package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view       viewservice.View
	data	   map[string]string
	isInit     bool
	ops        map[int64]bool
	readLock   *sync.Mutex
	writeLock  *sync.Mutex
}


func (pb *PBServer) isPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
	return pb.view.Backup == pb.me
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.readLock.Lock()
	defer pb.readLock.Unlock()
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	} else if val, ok := pb.data[args.Key]; ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return nil
}

func (pb *PBServer) GetAll(args *GetArgs, reply *GetReply) error {
	pb.readLock.Lock()
	// update view
	go func() {
		var view viewservice.View
		for view == pb.view {
			view, _ = pb.vs.Get()
		}
		pb.view = view
		reply.InitMapValue = pb.data
		reply.Err = OK
		pb.readLock.Unlock()
	}()

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	pb.writeLock.Lock()
	defer pb.writeLock.Unlock()
	pb.readLock.Lock()
	defer pb.readLock.Unlock()
	if !pb.isInit {
		return nil
	}



	_, ok := pb.ops[args.Id];
	if !ok || args.Forward && args.Op == PUT {

		// forward client request to Backup
		if pb.isPrimary() && len(pb.view.Backup) != 0 && !args.Forward {
			err := pb.forwardBackup(args)
			if err == ErrForward || err == "" {
				reply.Err = ErrForward
				return nil
			}
		}

		if args.Op == PUT && pb.isPrimary() {
			pb.data[args.Key] = args.Value
			reply.Err = OK
			pb.ops[args.Id] = true
		} else if args.Op == APPEND && pb.isPrimary() {
			pb.data[args.Key] += args.Value

			pb.ops[args.Id] = true

			reply.Err = OK
		} else if args.Forward && pb.isBackup() {
			if args.Op == PUT {
				pb.data[args.Key] = args.Value
			} else if args.Op == APPEND {
				pb.data[args.Key] += args.Value
			}
			pb.ops[args.Id] = true
			reply.Err = OK
		} else {
			reply.Err = ErrWrongServer
		}

	} else {
		reply.Err = ErrSameCommend
	}
	return nil
}

func (pb *PBServer) forwardBackup(args *PutAppendArgs) string {
	putArgs := PutAppendArgs{}
	putReply := PutAppendReply{}
	putArgs.Value = args.Value
	putArgs.Key = args.Key
	putArgs.Op = args.Op
	putArgs.Id = args.Id
	putArgs.Forward = true
	if len(pb.view.Backup) != 0 {
		call(pb.view.Backup, "PBServer.PutAppend", putArgs, &putReply)
	}
	if putReply.Err == OK || putReply.Err == ErrSameCommend {
		return OK
	} else {
		return ErrForward
	}
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.writeLock.Lock()
	pb.readLock.Lock()
	view, err := pb.vs.Get()
	pb.view = view
	pb.readLock.Unlock()
	pb.writeLock.Unlock()

	if err {
		if pb.isPrimary() {
			pb.isInit = true
		}
		if pb.isPrimary() || pb.isBackup() {

			pb.vs.Ping(pb.view.Viewnum)
		} else if len(pb.view.Backup) == 0 {
			pb.vs.Ping(0)
			pb.ops  = make(map[int64]bool)
			pb.isInit = false
		}
		if pb.isBackup() && !pb.isInit {
			pb.ops  = make(map[int64]bool)
			pb.backupInit()
		}
	}
}

func (pb *PBServer) backupInit() {
	args := GetArgs{}
	reply := GetReply{}
	call(pb.view.Primary, "PBServer.GetAll", args, &reply)

	if reply.Err == OK {
		pb.isInit = true
		pb.vs.Ping(pb.view.Viewnum)

		if reply.InitMapValue == nil {
			pb.data = make(map[string]string)
		} else {
			pb.data = reply.InitMapValue
		}
	} else {
		pb.isInit = false
	}

}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.data = make(map[string]string)
	pb.ops  = make(map[int64]bool)
	pb.readLock = &sync.Mutex{}
	pb.writeLock = &sync.Mutex{}
	pb.isInit = false
	view, _ := pb.vs.Ping(0)
	pb.view = view
	if pb.view.Primary == me {
		pb.vs.Ping(view.Viewnum)
		pb.isInit = true
	}
	if pb.view.Backup == me {
		pb.backupInit()
	}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
