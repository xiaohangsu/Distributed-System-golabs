package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currentView View
	prevView View
	primaryACK bool
	backupInit bool
	serverMap map[string]time.Time
	mutex	*sync.Mutex
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mutex.Lock()
	if args.Viewnum == 0 {
		if len(vs.currentView.Primary) == 0 && vs.currentView.Backup != args.Me {
			vs.currentView.Primary = args.Me
			vs.primaryACK = false
		} else if vs.currentView.Primary == args.Me {
			// restart treat Primary as dead
			if len(vs.currentView.Backup) > 0 {
				vs.currentView.Primary = vs.currentView.Backup
			}
			vs.currentView.Backup = ""
			vs.primaryACK = true
		} else {
			vs.primaryACK = true
		}
	} else if vs.currentView.Viewnum == args.Viewnum {
		if vs.currentView.Primary ==  args.Me {
			vs.primaryACK = true
		}
	} else if vs.currentView.Viewnum < args.Viewnum {
		vs.currentView.Backup = ""
		vs.currentView.Primary = args.Me
		vs.currentView.Viewnum = args.Viewnum
	} else {
		if vs.currentView.Primary == args.Me {
			vs.primaryACK = false
		}
	}

	if len(vs.currentView.Backup) == 0 && vs.currentView.Primary != args.Me {
		vs.currentView.Backup = args.Me
	}

	if vs.prevView != vs.currentView && vs.primaryACK {
		vs.currentView.Viewnum++
		vs.prevView = vs.currentView
		if vs.currentView.Primary == args.Me {
			vs.primaryACK = false
		}
		reply.View = vs.currentView
	} else {
		reply.View = vs.prevView
	}

	// Check Backup Init or not
	if vs.currentView.Backup == args.Me &&
		args.Viewnum == 0 {
		vs.backupInit = false
	} else if vs.currentView.Backup == args.Me &&
		args.Viewnum != 0 {
		vs.backupInit = true
	}

	// Update Server timestamp
	if vs.serverMap == nil {
		vs.serverMap = make(map[string]time.Time)
	}
	vs.serverMap[args.Me] = time.Now()
	vs.mutex.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mutex.Lock()
	reply.View = vs.currentView
	vs.mutex.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mutex.Lock()
	if vs.serverMap != nil &&
		len(vs.currentView.Primary) != 0 &&
		time.Now().Sub(vs.serverMap[vs.currentView.Primary]) > 3 * PingInterval {
		if vs.primaryACK {
			vs.currentView.Primary = ""
		}
		if len(vs.currentView.Backup) != 0 && vs.primaryACK {
			if vs.backupInit {
				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = ""
			}
		}

	}

	if vs.serverMap != nil && len(vs.currentView.Backup) != 0 &&
		time.Now().Sub(vs.serverMap[vs.currentView.Backup]) > 3 * PingInterval {
		vs.currentView.Backup = ""
	}
	vs.mutex.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.mutex = &sync.Mutex{}
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
