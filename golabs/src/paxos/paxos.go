package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import (
	"math/rand"
	"time"
)


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu           sync.Mutex
	l            net.Listener
	dead         int32 // for testing
	unreliable   int32 // for testing
	rpcCount     int32 // for testing
	peers        []string
	me           int // index into peers[]
	// Your data here.
	maxSeq       int
	minSeq       int
	dones	     map[int]int // me to Done
	instances    map[int]*Instance
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) debug(method string, seq int, v interface{}, err Error) {
	state := px.instances[seq]
	fmt.Println("-------------------------------------------------------")
	fmt.Printf("Server %d Method: %s, Error: %s\n", px.me, method, err)
	fmt.Printf("Seq: %d, Value: %s\n", seq, v)
	fmt.Printf("State: n_p : %d, n_a : %d, v: %s, status: %d \n", state.n_p, state.n_a, state.v, state.status)
	fmt.Println("-------------------------------------------------------")
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.


	go func() {
		if px.Min() > seq {
			return
		}

		for {
			n, value, proposeErr := px.propose(seq, v)
			//fmt.Printf("Seq %d Propose Number: %d, Propose Error: %s, Value: %s  - px.me: %d \n",
			//	seq, n, proposeErr, value, px.me)
			if proposeErr == OK {
				acceptErr := px.accept(n, seq, value)
				//fmt.Printf("Seq %d Accept Number: %d, Accept Error: %s, Value: %s  - px.me: %d \n",
				//	seq, n, proposeErr, value, px.me)
				if acceptErr == OK {
					px.decide(n, seq, value)
				}
			}

			status, _ := px.Status(seq)
			if status == Decided {
				break
			}
		}
	}()
}

func (px *Paxos) propose(seq int,
			 v interface{}) (int64, interface{}, Error) {
	n := time.Now().UnixNano() // n_p highest proposal seen for this instance
	proposeAgree := 0
	var value interface{}
	value = v
	var maxNA int64
	maxNA = -1
	proposeArgs := ProposeArgs{}
	proposeArgs.Seq = seq
	proposeArgs.N = n
	for _, peer := range px.peers {

		var proposeReply = ProposeReply{Err:ServerUnreached, ValueAccpet:nil, NAccpet:-1}
		// skip local
		if peer == px.peers[px.me] {
			px.Propose(&proposeArgs, &proposeReply)

		} else {
			call(peer, "Paxos.Propose", proposeArgs, &proposeReply)
		}

		if proposeReply.Err == OK {
			proposeAgree++
		}
		if (maxNA < proposeReply.NAccpet) {
			maxNA = proposeReply.NAccpet
			value = proposeReply.ValueAccpet
		}
	}

	if proposeAgree >= (len(px.peers) / 2 + 1) && px.minSeq <= seq {
		return n, value, OK
	}
	return -1, value, FailedOnMajority
}

func (px *Paxos) accept(n int64, seq int, v interface{}) Error {
	acceptAgree := 0
	acceptArgs := AcceptArgs{}
	acceptArgs.Seq = seq
	acceptArgs.Value = v
	acceptArgs.N = n
	for _, peer := range px.peers {
		var acceptReply AcceptReply

		// skip local
		if peer == px.peers[px.me] {
			px.Accept(&acceptArgs, &acceptReply)
		} else {
			call(peer, "Paxos.Accept", acceptArgs, &acceptReply)
		}

		if acceptReply.Err == OK {
			acceptAgree++
		}
	}

	if acceptAgree >= (len(px.peers) / 2 + 1) && px.minSeq <= seq {
		return OK
	}
	return FailedOnMajority
}

func (px *Paxos) decide(n int64, seq int, v interface{}) Error {
	px.mu.Lock()
	decideArgs := DecideArgs{}
	decideArgs.Seq = seq
	decideArgs.Value = v
	decideArgs.Me = px.me
	decideArgs.N = n
	decideArgs.MinSeq = px.dones[px.me]
	px.mu.Unlock()

	for _, peer := range px.peers {
		var decideReply DecideReply
		if peer == px.peers[px.me] {
			px.Decided(&decideArgs, &decideReply)
		} else {
			call(peer, "Paxos.Decided", decideArgs, &decideReply)
		}
	}
	return OK
}

func (px *Paxos) Propose(args *ProposeArgs, reply *ProposeReply) error {

	if args.Seq < px.minSeq { // propose Seq smaller than minSeq
		reply.Err = ErrReject
		reply.N = args.N
		reply.NAccpet = -1
		return nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()
	instance := px.getIntance(args.Seq)

	if args.N > instance.n_p {
		instance.n_p = args.N
		reply.Err = OK
		reply.NAccpet = instance.n_a
		reply.ValueAccpet = instance.v
	} else {
		reply.Err = ErrReject
		reply.N = instance.n_p
		reply.NAccpet = instance.n_a
		reply.ValueAccpet = instance.v
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.Seq < px.minSeq {	// check args.Seq is smaller than px.dones[px.me]
		reply.Err = ErrReject
		return nil
	}

	instance := px.getIntance(args.Seq)

	if args.N >= instance.n_p {
		px.instances[args.Seq].n_p = args.N
		px.instances[args.Seq].n_a = args.N
		px.instances[args.Seq].v   = args.Value
		reply.Err = OK
	} else {
		reply.Err = ErrReject
	}
	return nil
}

func (px *Paxos) Decided (args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	//fmt.Printf("Server %d Decided %v \n", px.me, args)
	defer px.mu.Unlock()
	if args.Seq < px.dones[px.me] {
		return nil
	}

	instance := px.getIntance(args.Seq)
	instance.v = args.Value
	instance.status = Decided
	instance.n_p = args.N
	instance.n_a = args.N
	reply.Err = OK
	// make sure the minSeq is larger than previous heard

	if px.dones[args.Me] < args.MinSeq {
		px.dones[args.Me] = args.MinSeq
	}
	return nil
}

// Safely get instance
func (px *Paxos) getIntance(seq int) *Instance {
	if px.maxSeq < seq {
		px.maxSeq = seq
	}

	if _, exist := px.instances[seq]; !exist {
		px.instances[seq] = InstanceInit()
	}
	return px.instances[seq]
}


//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.dones[px.me] < seq {
		px.dones[px.me] = seq
	}

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	px.minSeq = px.dones[px.me]
	for i := range px.dones {
		if px.dones[i] < px.minSeq {
			px.minSeq = px.dones[i]
		}

	}

	for k, instance := range px.instances {
		if k <= px.minSeq {
			instance.v = nil
			px.instances[k] = instance
			delete(px.instances, k)
		}
	}
	return px.minSeq + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()
	instance := px.getIntance(seq)
	if px.maxSeq < seq {
		return Pending, nil
	}
	return instance.status, instance.v
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.mu = sync.Mutex{}
	px.instances = make(map[int]*Instance)
	px.dones = make(map[int]int)
	px.maxSeq = -1
	px.minSeq = -1
	for i := 0; i < len(px.peers); i++ {
		px.dones[i] = -1;
	}
	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
