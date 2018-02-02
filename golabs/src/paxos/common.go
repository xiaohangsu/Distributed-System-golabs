package paxos

const (
	OK		= "OK"
	ErrLowSeq	= "ErrLowSeq"
	ErrReject	= "ErrReject"
	ErrServerDead   = "ErrServerDead"
	FailedOnMajority= "FailedOnMajority"
	UnCatchError    = "UnCatchError"
	ServerUnreached = "ServerUnreached"
)

type Instance struct {
	n_p int64
	n_a int64
	v   interface{}
	status Fate
}

func InstanceInit() *Instance {
	instance := new(Instance)
	instance.n_p = -1
	instance.status = Pending
	instance.v = nil
	instance.n_a = -1
	return instance
}

type Error string

type ProposeArgs struct {
	N   	int64
	Seq 	int
}


type ProposeReply struct {
	Err 		Error
	N 		int64
	NAccpet 	int64
	ValueAccpet 	interface{}
}

type AcceptArgs struct {
	Seq	int
	N	int64
	Value   interface{}
}

type AcceptReply struct {
	Err 	Error
}

type DecideArgs struct {
	Value		interface{}
	Seq     	int
	MinSeq  	int
	Me              int
	N 		int64
}

type DecideReply struct {
	Err 	Error
}