package pbservice


const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrSameCommend = "ErrSameCommend"
	ErrForward     = "ErrForward"
	PUT	       = "Put"
	PUTAPPEND      = "PutAppend"
	GET            = "Get"
	INIT           = "INIT"
	APPEND         = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	// You'll have to add definitions here.
	Op    		string
	Id     		int64
	Forward		bool
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	InitMapValue map[string]string
}


// Your RPC definitions here.
